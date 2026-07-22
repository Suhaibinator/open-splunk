package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultMaximumRequestBytes        = int64(128 << 10)
	defaultMaximumPageSize            = uint32(1_000)
	defaultMaximumConcurrentRequests  = 64
	defaultMaximumConcurrentResponses = 8
	defaultRouteTimeout               = 15 * time.Second
	defaultSearchTimeout              = 2 * time.Minute
	defaultResultRetention            = 15 * time.Minute
	maximumRequestBytes               = int64(128 << 10)
	maximumSmallRequestBytes          = int64(16 << 10)
	maximumTransportPageSize          = uint32(10_000)
	maximumConcurrentResponses        = 256
	maximumConcurrentRequests         = 1_024
	maximumIdentityBytes              = 255
	maximumBootstrapApps              = 256
)

// SearchJobs is the scoped search-job surface exposed to the browser API.
// Manager satisfies this interface directly.
type SearchJobs interface {
	Create(context.Context, searchjobs.CreateRequest) (searchjobs.Job, error)
	GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error)
	ResultsFor(searchjobs.AccessScope, string, searchjobs.PageRequest) (searchjobs.ResultPage, error)
	CancelFor(searchjobs.AccessScope, string) error
}

// IndexCatalog supplies the live index authorization and bootstrap view.
// control.DB satisfies this interface directly.
type IndexCatalog interface {
	ListIndexes(context.Context) ([]control.Index, error)
	GetIndexByName(context.Context, string) (control.Index, error)
}

// IndexAdministration is the mutable control-plane surface used by the index
// provisioning API. control.DB satisfies this interface directly.
type IndexAdministration interface {
	CreateIndex(context.Context, control.IndexDefinition) (control.Index, error)
	GetIndex(context.Context, string) (control.Index, error)
	GetIndexByName(context.Context, string) (control.Index, error)
	ListIndexes(context.Context) ([]control.Index, error)
	UpdateIndex(context.Context, string, uint64, control.IndexDefinition) (control.Index, error)
	SetIndexState(context.Context, string, uint64, control.IndexState) (control.Index, error)
}

// IngestionTokenAdministration is the secret-safe collector credential
// surface exposed to the browser API. Only Create returns a one-time Secret;
// every other method returns metadata which cannot authenticate a collector.
type IngestionTokenAdministration interface {
	CreateCollectorToken(context.Context, auth.CreateCollectorTokenRequest) (auth.IssuedCollectorToken, error)
	GetCollectorToken(context.Context, string) (auth.CollectorToken, error)
	ListCollectorTokens(context.Context) ([]auth.CollectorToken, error)
	UpdateCollectorToken(context.Context, string, uint64, auth.UpdateCollectorTokenRequest) (auth.CollectorToken, error)
	RevokeCollectorToken(context.Context, string, uint64) (auth.CollectorToken, error)
}

// SavedSearches is the owner-scoped saved-search surface exposed to the
// browser API. savedobjects.Store satisfies this interface directly. Keeping
// the authenticated owner outside every protobuf request prevents callers
// from selecting another user's namespace in the trusted single-user release.
type SavedSearches interface {
	Create(context.Context, savedobjects.AccessScope, *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error)
	Get(context.Context, savedobjects.AccessScope, string) (*opensplunkv1.SavedSearch, error)
	List(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error)
	Update(context.Context, savedobjects.AccessScope, string, uint64, *opensplunkv1.SavedSearchDefinition, *fieldmaskpb.FieldMask) (*opensplunkv1.SavedSearch, error)
	Duplicate(context.Context, savedobjects.AccessScope, string, string, *string) (*opensplunkv1.SavedSearch, error)
	Delete(context.Context, savedobjects.AccessScope, string, uint64) error
}

// BootstrapConfig contains build information and static workspace summaries.
// Index summaries are always loaded from IndexCatalog so authorization and the
// UI bootstrap cannot drift apart.
type BootstrapConfig struct {
	ServerVersion           string
	APIVersion              string
	SPLCompatibilityVersion string
	SearchWebSocketPath     string
	Features                []opensplunkv1.ServerFeature
	Apps                    []*opensplunkv1.AppSummary
	SelectedAppID           string
	MaximumPreviewRows      uint32
	MaximumSubscriptions    uint32
	MaximumWebSocketBytes   uint64
	MaximumExportRows       uint64
	MaximumExportBytes      uint64
	DefaultSearchTimeout    time.Duration
	SearchResultRetention   time.Duration
}

// Config composes the trusted-network browser API and embedded static UI.
// OwnerID and TenantID are fixed process identities for the initial
// single-user release; authentication can replace them without changing the
// search-job ownership boundary.
type Config struct {
	SearchJobs                 SearchJobs
	Indexes                    IndexCatalog
	IndexAdmin                 IndexAdministration
	IngestionTokens            IngestionTokenAdministration
	SavedSearches              SavedSearches
	WebUI                      fs.FS
	Bootstrap                  BootstrapConfig
	OwnerID                    string
	TenantID                   string
	MaximumRequestBytes        int64
	MaximumPageSize            uint32
	MaximumConcurrentRequests  int
	MaximumConcurrentResponses int
	RouteTimeout               time.Duration
	Now                        func() time.Time
	// AdministrativeAllowedHosts is an explicit browser trust boundary for
	// unauthenticated index and ingestion-token administration. Values are host
	// names or IP literals without paths. Empty defaults to loopback names only.
	AdministrativeAllowedHosts []string
}

type apiHandler struct {
	jobs              SearchJobs
	indexes           IndexCatalog
	indexAdmin        IndexAdministration
	ingestionTokens   IngestionTokenAdministration
	savedSearches     SavedSearches
	ownerID           string
	tenantID          string
	maximumPageSize   uint32
	bootstrap         BootstrapConfig
	now               func() time.Time
	requestGate       chan struct{}
	serializationGate chan struct{}
	adminCursorKey    [32]byte
	adminAllowedHosts map[string]struct{}
}

// NewHandler constructs the complete HTTP handler. API paths are dispatched
// before the SPA handler, including unknown API paths, so frontend fallback can
// never conceal an unavailable or misspelled backend route.
func NewHandler(config Config) (http.Handler, error) {
	if isNilDependency(config.SearchJobs) {
		return nil, errors.New("create server handler: search job service is required")
	}
	if isNilDependency(config.Indexes) {
		return nil, errors.New("create server handler: index catalog is required")
	}
	indexAdmin := config.IndexAdmin
	if isNilDependency(indexAdmin) {
		if inferred, ok := config.Indexes.(IndexAdministration); ok && !isNilDependency(inferred) {
			indexAdmin = inferred
		} else {
			indexAdmin = nil
		}
	}
	ingestionTokens := config.IngestionTokens
	if isNilDependency(ingestionTokens) {
		ingestionTokens = nil
	}
	if isNilDependency(config.SavedSearches) {
		return nil, errors.New("create server handler: saved search service is required")
	}
	if config.WebUI == nil {
		return nil, errors.New("create server handler: web UI filesystem is required")
	}
	if config.MaximumRequestBytes < 0 || config.MaximumRequestBytes > maximumRequestBytes {
		return nil, fmt.Errorf("create server handler: maximum request size must be between 1 and %d bytes", maximumRequestBytes)
	}
	requestBytes := config.MaximumRequestBytes
	if requestBytes == 0 {
		requestBytes = defaultMaximumRequestBytes
	}
	pageSize := config.MaximumPageSize
	if pageSize == 0 {
		pageSize = defaultMaximumPageSize
	}
	if pageSize > maximumTransportPageSize {
		return nil, fmt.Errorf("create server handler: maximum page size cannot exceed %d", maximumTransportPageSize)
	}
	concurrentResponses := config.MaximumConcurrentResponses
	if concurrentResponses < 0 || concurrentResponses > maximumConcurrentResponses {
		return nil, fmt.Errorf("create server handler: maximum concurrent responses must be between 1 and %d", maximumConcurrentResponses)
	}
	if concurrentResponses == 0 {
		concurrentResponses = defaultMaximumConcurrentResponses
	}
	concurrentRequests := config.MaximumConcurrentRequests
	if concurrentRequests < 0 || concurrentRequests > maximumConcurrentRequests {
		return nil, fmt.Errorf("create server handler: maximum concurrent requests must be between 1 and %d", maximumConcurrentRequests)
	}
	if concurrentRequests == 0 {
		concurrentRequests = defaultMaximumConcurrentRequests
	}
	routeTimeout := config.RouteTimeout
	if routeTimeout < 0 {
		return nil, errors.New("create server handler: route timeout cannot be negative")
	}
	if routeTimeout == 0 {
		routeTimeout = defaultRouteTimeout
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	ownerID := strings.TrimSpace(config.OwnerID)
	if ownerID == "" {
		ownerID = "single-user"
	}
	tenantID := strings.TrimSpace(config.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}
	if validateBoundedIdentifier(ownerID, maximumSavedSearchOwnerBytes, false) != nil || len(tenantID) > maximumIdentityBytes || !utf8.ValidString(tenantID) {
		return nil, errors.New("create server handler: owner or tenant identity is invalid")
	}
	bootstrap, err := normalizeBootstrap(config.Bootstrap)
	if err != nil {
		return nil, err
	}
	bootstrap.Features = featuresForServices(bootstrap.Features, indexAdmin != nil, ingestionTokens != nil)
	adminAllowedHosts, err := normalizeAdministrativeAllowedHosts(config.AdministrativeAllowedHosts, indexAdmin != nil || ingestionTokens != nil)
	if err != nil {
		return nil, fmt.Errorf("create server handler: %w", err)
	}
	spa, err := newSPAHandler(config.WebUI)
	if err != nil {
		return nil, fmt.Errorf("create server handler: %w", err)
	}
	var adminCursorKey [32]byte
	if indexAdmin != nil || ingestionTokens != nil {
		if _, err := rand.Read(adminCursorKey[:]); err != nil {
			return nil, errors.New("create server handler: secure randomness unavailable for administrative cursors")
		}
	}

	api := &apiHandler{
		jobs:              config.SearchJobs,
		indexes:           config.Indexes,
		indexAdmin:        indexAdmin,
		ingestionTokens:   ingestionTokens,
		savedSearches:     config.SavedSearches,
		ownerID:           ownerID,
		tenantID:          tenantID,
		maximumPageSize:   pageSize,
		bootstrap:         bootstrap,
		now:               now,
		requestGate:       make(chan struct{}, concurrentRequests),
		serializationGate: make(chan struct{}, concurrentResponses),
		adminCursorKey:    adminCursorKey,
		adminAllowedHosts: adminAllowedHosts,
	}
	apiRouter := api.newRouter(requestBytes, routeTimeout)
	apiRoutes := map[string]struct{}{
		"/api/v1/system/bootstrap":         {},
		"/api/v1/search/jobs/create":       {},
		"/api/v1/search/jobs/get":          {},
		"/api/v1/search/jobs/results":      {},
		"/api/v1/search/jobs/cancel":       {},
		"/api/v1/saved-searches/create":    {},
		"/api/v1/saved-searches/get":       {},
		"/api/v1/saved-searches/list":      {},
		"/api/v1/saved-searches/update":    {},
		"/api/v1/saved-searches/duplicate": {},
		"/api/v1/saved-searches/delete":    {},
	}
	if api.indexAdmin != nil {
		for _, path := range []string{
			"/api/v1/indexes/create",
			"/api/v1/indexes/get",
			"/api/v1/indexes/list",
			"/api/v1/indexes/update",
			"/api/v1/indexes/state/set",
		} {
			apiRoutes[path] = struct{}{}
		}
	}
	if api.ingestionTokens != nil {
		for _, path := range []string{
			"/api/v1/ingestion-tokens/create",
			"/api/v1/ingestion-tokens/get",
			"/api/v1/ingestion-tokens/list",
			"/api/v1/ingestion-tokens/update",
			"/api/v1/ingestion-tokens/revoke",
		} {
			apiRoutes[path] = struct{}{}
		}
	}
	apiBoundary := exactAPIRoutes(api.protectAdministrativeRoutes(apiRouter), apiRoutes)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.Header().Set("Cache-Control", "no-store")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	// Register both forms. Without the exact /api pattern, a request for /api
	// itself could otherwise reach the SPA's index document.
	mux.Handle("/api", apiBoundary)
	mux.Handle("/api/", apiBoundary)
	mux.Handle("/", spa)
	return mux, nil
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func exactAPIRoutes(next http.Handler, routes map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if _, exists := routes[request.URL.Path]; !exists {
			writeAPIError(response, http.StatusNotFound, "API route not found")
			return
		}
		if request.Method != http.MethodPost {
			response.Header().Set("Allow", http.MethodPost)
			writeAPIError(response, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		next.ServeHTTP(response, request)
	})
}

func writeAPIError(response http.ResponseWriter, status int, message string) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_, _ = fmt.Fprintf(response, "{\"error\":{\"message\":%q}}\n", message)
}

func normalizeBootstrap(config BootstrapConfig) (BootstrapConfig, error) {
	result := config
	result.ServerVersion = strings.TrimSpace(result.ServerVersion)
	if result.ServerVersion == "" {
		result.ServerVersion = "dev"
	}
	result.APIVersion = strings.TrimSpace(result.APIVersion)
	if result.APIVersion == "" {
		result.APIVersion = "v1"
	}
	result.SPLCompatibilityVersion = strings.TrimSpace(result.SPLCompatibilityVersion)
	if result.SPLCompatibilityVersion == "" {
		result.SPLCompatibilityVersion = "tier-1-dev"
	}
	if result.DefaultSearchTimeout < 0 || result.SearchResultRetention < 0 {
		return BootstrapConfig{}, errors.New("create server handler: bootstrap durations cannot be negative")
	}
	if result.DefaultSearchTimeout == 0 {
		result.DefaultSearchTimeout = defaultSearchTimeout
	}
	if result.SearchResultRetention == 0 {
		result.SearchResultRetention = defaultResultRetention
	}
	if len(result.Features) == 0 {
		result.Features = []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunkv1.ServerFeature_SERVER_FEATURE_SAVED_SEARCHES,
		}
	} else {
		result.Features = slices.Clone(result.Features)
	}
	if len(result.Apps) > maximumBootstrapApps {
		return BootstrapConfig{}, fmt.Errorf("create server handler: bootstrap apps cannot exceed %d", maximumBootstrapApps)
	}
	apps := make([]*opensplunkv1.AppSummary, len(result.Apps))
	for index, app := range result.Apps {
		if app == nil {
			return BootstrapConfig{}, errors.New("create server handler: bootstrap app cannot be nil")
		}
		apps[index] = proto.Clone(app).(*opensplunkv1.AppSummary)
	}
	result.Apps = apps
	return result, nil
}

func featuresForServices(features []opensplunkv1.ServerFeature, _, _ bool) []opensplunkv1.ServerFeature {
	// The current handlers intentionally expose only the clean-install
	// provisioning subset of these API families. Do not advertise either broad
	// capability until every route in the corresponding proto family exists.
	result := make([]opensplunkv1.ServerFeature, 0, len(features))
	for _, feature := range features {
		if feature != opensplunkv1.ServerFeature_SERVER_FEATURE_INDEX_ADMIN &&
			feature != opensplunkv1.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN {
			result = append(result, feature)
		}
	}
	return result
}

func (handler *apiHandler) newRouter(maximumRequestBytes int64, routeTimeout time.Duration) http.Handler {
	noAuth := router.NoAuth
	protobufMiddleware := requireProtobufContentType
	requestMiddleware := handler.boundRequests
	deadlineMiddleware := withSynchronousDeadline(routeTimeout)
	smallRequestBytes := min(maximumRequestBytes, maximumSmallRequestBytes)

	routes := []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse]{
			Path: "/system/bootstrap", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse](), Handler: handler.getSystemBootstrap,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSystemBootstrapRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse, string, struct{}](router.RouteConfig[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse]{
			Path: "/search/jobs/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse](), Handler: handler.createSearchJob,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.CreateSearchJobRequest],
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse]{
			Path: "/search/jobs/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse](), Handler: handler.getSearchJob,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSearchJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetSearchResultsRequest, *serializedSearchResultsResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSearchResultsRequest, *serializedSearchResultsResponse]{
			Path: "/search/jobs/results", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchResultsCodec(), Handler: handler.getSearchResults,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSearchResultsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse, string, struct{}](router.RouteConfig[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse]{
			Path: "/search/jobs/cancel", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse](), Handler: handler.cancelSearchJob,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.CancelSearchJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse, string, struct{}](router.RouteConfig[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse]{
			Path: "/saved-searches/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse](), Handler: handler.createSavedSearch,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.CreateSavedSearchRequest],
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse]{
			Path: "/saved-searches/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse](), Handler: handler.getSavedSearch,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.ListSavedSearchesRequest, *serializedSavedSearchListResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListSavedSearchesRequest, *serializedSavedSearchListResponse]{
			Path: "/saved-searches/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSavedSearchListCodec(), Handler: handler.listSavedSearches,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListSavedSearchesRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse, string, struct{}](router.RouteConfig[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse]{
			Path: "/saved-searches/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse](), Handler: handler.updateSavedSearch,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.UpdateSavedSearchRequest],
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse, string, struct{}](router.RouteConfig[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse]{
			Path: "/saved-searches/duplicate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse](), Handler: handler.duplicateSavedSearch,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.DuplicateSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse, string, struct{}](router.RouteConfig[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse]{
			Path: "/saved-searches/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse](), Handler: handler.deleteSavedSearch,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.DeleteSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
	if handler.indexAdmin != nil {
		routes = append(routes, handler.indexAdministrationRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.ingestionTokens != nil {
		routes = append(routes, handler.ingestionTokenRoutes(noAuth, maximumRequestBytes, smallRequestBytes)...)
	}

	return router.NewRouter[string, struct{}](router.RouterConfig{
		ServiceName: "open-splunk-server",
		// SRouter's built-in timeout returns while its handler goroutine may
		// continue using services. Keep it disabled and apply a synchronous
		// context deadline so http.Server.Shutdown owns every handler lifetime.
		GlobalTimeout:     0,
		GlobalMaxBodySize: maximumRequestBytes,
		SubRouters: []router.SubRouterConfig{{
			PathPrefix:  "/api/v1",
			AuthLevel:   &noAuth,
			Middlewares: []sroutercommon.Middleware{disableAPICaching, protobufMiddleware, requestMiddleware, deadlineMiddleware},
			Routes:      routes,
		}},
	}, nil, nil)
}

func disableAPICaching(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("Pragma", "no-cache")
		next.ServeHTTP(response, request)
	})
}

func requireProtobufContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			contentType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
			if err != nil || !strings.EqualFold(contentType, "application/x-protobuf") {
				response.Header().Set("Content-Type", "application/json; charset=utf-8")
				response.WriteHeader(http.StatusUnsupportedMediaType)
				_, _ = response.Write([]byte("{\"error\":{\"message\":\"Content-Type must be application/x-protobuf\"}}\n"))
				return
			}
		}
		next.ServeHTTP(response, request)
	})
}

func (handler *apiHandler) boundRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		select {
		case handler.requestGate <- struct{}{}:
			defer func() { <-handler.requestGate }()
			next.ServeHTTP(response, request)
		default:
			writeBusyResponse(response, "API request capacity is exhausted")
		}
	})
}

func withSynchronousDeadline(timeout time.Duration) sroutercommon.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			ctx, cancel := context.WithTimeout(request.Context(), timeout)
			defer cancel()
			next.ServeHTTP(response, request.WithContext(ctx))
		})
	}
}

func (handler *apiHandler) acquireSerialization() (func(), bool) {
	select {
	case handler.serializationGate <- struct{}{}:
		return func() { <-handler.serializationGate }, true
	default:
		return nil, false
	}
}

func writeBusyResponse(response http.ResponseWriter, message string) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.Header().Set("Retry-After", "1")
	response.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(response, "{\"error\":{\"message\":%q}}\n", message)
}

// serializedSearchResultsResponse transfers ownership of one serialization
// permit from the typed handler to its codec. Keeping the permit through
// protobuf marshaling and the response write bounds both detached result pages
// and wire buffers, while acquiring it after request decoding means slow
// uploads cannot starve normal result readers.
type serializedSearchResultsResponse struct {
	message *opensplunkv1.GetSearchResultsResponse
	ctx     context.Context
	release func()
}

type serializedSearchResultsCodec struct {
	inner codec.Codec[*opensplunkv1.GetSearchResultsRequest, *opensplunkv1.GetSearchResultsResponse]
}

func newSerializedSearchResultsCodec() *serializedSearchResultsCodec {
	return &serializedSearchResultsCodec{
		inner: codec.NewProtoCodec[*opensplunkv1.GetSearchResultsRequest, *opensplunkv1.GetSearchResultsResponse](),
	}
}

func (codec *serializedSearchResultsCodec) NewRequest() *opensplunkv1.GetSearchResultsRequest {
	return codec.inner.NewRequest()
}

func (codec *serializedSearchResultsCodec) Decode(request *http.Request) (*opensplunkv1.GetSearchResultsRequest, error) {
	return codec.inner.Decode(request)
}

func (codec *serializedSearchResultsCodec) DecodeBytes(data []byte) (*opensplunkv1.GetSearchResultsRequest, error) {
	return codec.inner.DecodeBytes(data)
}

func (codec *serializedSearchResultsCodec) Encode(response http.ResponseWriter, result *serializedSearchResultsResponse) error {
	if result == nil || result.release == nil {
		return errors.New("search result serialization permit is missing")
	}
	defer result.release()
	if result.message == nil {
		return errors.New("search result response is missing")
	}
	if result.ctx != nil {
		if err := result.ctx.Err(); err != nil {
			return err
		}
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	// Marshal can be the most expensive remaining step for a maximum-size
	// page. Re-check the synchronous deadline before committing a 200 response.
	if result.ctx != nil {
		if err := result.ctx.Err(); err != nil {
			return err
		}
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}
