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

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildmetadata"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsuggestions"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	apiV1PathPrefix                   = "/api/v1"
	searchJobsListRoute               = "/search/jobs/list"
	searchJobsListPath                = apiV1PathPrefix + searchJobsListRoute
	exportJobsListRoute               = "/search/exports/list"
	exportJobsListPath                = apiV1PathPrefix + exportJobsListRoute
	searchFieldsListRoute             = "/search/jobs/fields/list"
	searchFieldsListPath              = apiV1PathPrefix + searchFieldsListRoute
	indexFieldsListRoute              = "/indexes/fields/list"
	indexFieldsListPath               = apiV1PathPrefix + indexFieldsListRoute
	searchFieldSummaryRoute           = "/search/jobs/field-summary"
	searchFieldSummaryPath            = apiV1PathPrefix + searchFieldSummaryRoute
	searchTimelinePath                = "/api/v1/search/jobs/timeline"
	searchInspectionRoute             = "/search/jobs/inspect"
	searchInspectionPath              = apiV1PathPrefix + searchInspectionRoute
	auditEventsListRoute              = "/audit/events/list"
	auditEventsListPath               = apiV1PathPrefix + auditEventsListRoute
	searchWebSocketPath               = "/api/v1/search/ws"
	defaultMaximumRequestBytes        = int64(128 << 10)
	defaultMaximumPageSize            = uint32(1_000)
	defaultMaximumConcurrentRequests  = 64
	defaultMaximumConcurrentResponses = 8
	defaultMaximumConcurrentDownloads = 16
	defaultRouteTimeout               = 15 * time.Second
	defaultSearchTimeout              = 2 * time.Minute
	defaultResultRetention            = 15 * time.Minute
	maximumRequestBytes               = int64(128 << 10)
	maximumSmallRequestBytes          = int64(16 << 10)
	maximumTransportPageSize          = uint32(10_000)
	maximumConcurrentResponses        = 256
	maximumConcurrentRequests         = 1_024
	maximumConcurrentDownloads        = 256
	maximumIdentityBytes              = 255
	maximumBootstrapApps              = 256
	// A field profile may contain two 8,720-byte names. Keeping pages at 1,000
	// guarantees even the worst valid protobuf response remains below the
	// transport's independent 32 MiB fail-closed cap.
	maximumSearchFieldPageSize    = uint32(1_000)
	maximumSearchTimelineBuckets  = uint32(10_000)
	maximumSearchPreviewRows      = uint32(1_000)
	maximumWebSocketSubscriptions = uint32(256)
	minimumWebSocketFrameBytes    = uint64(1 << 10)
	maximumWebSocketFrameBytes    = uint64(1 << 20)
	runtimeReadinessTimeout       = time.Second
)

// SearchJobs is the scoped search-job surface exposed to the browser API.
// Manager satisfies this interface directly.
type SearchJobs interface {
	Create(context.Context, searchjobs.CreateRequest) (searchjobs.Job, error)
	Validate(context.Context, searchjobs.ValidateRequest) (searchjobs.ValidationResult, error)
	GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error)
	ListPageFor(context.Context, searchjobs.AccessScope, searchjobs.JobListRequest) (searchjobs.JobListPage, error)
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
	ListIndexPage(context.Context, control.IndexListRequest) (control.IndexListResult, error)
	UpdateIndex(context.Context, string, uint64, control.IndexDefinition) (control.Index, error)
	SetIndexState(context.Context, string, uint64, control.IndexState) (control.Index, error)
	DeleteIndex(context.Context, string, uint64, string) (string, error)
}

// IndexStatistics reads already-resolved logical indexes from the native event
// store. Echoed result scopes let the browser boundary reject dependency
// responses produced for a different tenant, index, snapshot, or measurement
// instant.
type IndexStatistics interface {
	GetIndexStatistics(
		context.Context,
		clickhouse.IndexStatisticsRequest,
	) (clickhouse.IndexStatisticsResult, error)
	GetIndexStatisticsBatch(
		context.Context,
		clickhouse.IndexStatisticsBatchRequest,
	) ([]clickhouse.IndexStatisticsResult, error)
}

// IndexStatisticsSnapshotter captures the largest committed visibility
// sequence before an index-statistics query starts.
type IndexStatisticsSnapshotter interface {
	VisibilityCutoff(context.Context) (uint64, error)
}

// RuntimeReadiness checks whether the server's ordinary ClickHouse runtime
// session can still reach its dependency. Implementations must honor the
// supplied context and perform no mutation; clickhouse-go's Conn satisfies the
// interface directly through Ping.
type RuntimeReadiness interface {
	Ping(context.Context) error
}

// IndexDataDeletionAdmission durably admits one physical index deletion in
// the trusted control plane. The tenant scope must be supplied by the server,
// never by browser input.
type IndexDataDeletionAdmission interface {
	BeginIndexDataDeletion(
		context.Context,
		control.IndexDataDeletionScope,
		string,
		uint64,
		string,
	) (control.IndexDeletionOperation, error)
}

// IndexDataDeletionWaker requests prompt reconciliation after a durable
// physical-deletion admission. Implementations must be nonblocking and safe
// during shutdown; periodic recovery remains the correctness backstop.
type IndexDataDeletionWaker interface {
	Wake()
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

// AuditEvents is the bounded, administrator-only successful-event journal.
// Tenant identity comes from the authenticated browser principal and is never
// accepted from protobuf input.
type AuditEvents interface {
	List(context.Context, string, audit.ListRequest) (audit.ListPage, error)
}

// SearchAttemptAuditEvents is the bounded, administrator-only journal of every
// admitted search attempt. Tenant identity comes from the authenticated
// browser principal and is never accepted from protobuf input.
type SearchAttemptAuditEvents interface {
	List(
		context.Context,
		string,
		searchaudit.ListRequest,
	) (searchaudit.ListPage, error)
}

// CollectorAdministration is the complete tenant-scoped fleet surface exposed
// to an authenticated browser administrator. Implementations must source
// liveness from trusted process state, never from protobuf input, and must not
// hydrate operational telemetry after committing an administrator mutation.
type CollectorAdministration interface {
	Get(
		context.Context,
		collectorfleet.Scope,
		string,
	) (collectorfleet.CatalogEntry, error)
	List(
		context.Context,
		collectorfleet.Scope,
		collectorfleet.ListRequest,
	) (collectorfleet.ListResult, error)
	UpdateDisplayName(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		*string,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
	SetAdministrativeState(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		collectorfleet.AdministrativeState,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
}

// AppAdministration is the complete administrator-only app-workspace surface.
// Its transport-independent types keep the browser boundary decoupled from
// control-plane persistence. Implementations must scope every operation by
// TenantID and treat ActorID as immutable audit context. ListApps must apply
// the supplied bounded page request in storage rather than loading an
// unbounded tenant into memory.
type AppAdministration interface {
	CreateApp(context.Context, AppAdministrationScope, AppAdministrationDefinition) (AppAdministrationWorkspace, error)
	GetApp(context.Context, AppAdministrationScope, AppAdministrationSelector) (AppAdministrationWorkspace, error)
	ListApps(context.Context, AppAdministrationScope, AppAdministrationListRequest) (AppAdministrationListResult, error)
	UpdateApp(context.Context, AppAdministrationScope, AppAdministrationSelector, uint64, AppAdministrationDefinition) (AppAdministrationWorkspace, error)
	SetAppState(context.Context, AppAdministrationScope, AppAdministrationSelector, uint64, AppAdministrationState) (AppAdministrationWorkspace, error)
	DeleteApp(context.Context, AppAdministrationScope, AppAdministrationSelector, uint64, string) (string, error)
}

// AppCatalogSummary is one detached active workspace projection for the
// ordinary browser bootstrap. Lifecycle state is intentionally omitted:
// ListActiveApps must return active workspaces only.
type AppCatalogSummary struct {
	AppID             string
	Slug              string
	DisplayName       string
	DefaultIndexNames []string
}

// AppCatalogResult is the complete bounded active catalog for one tenant.
// Complete must be false when more rows exist than the caller's maximum.
type AppCatalogResult struct {
	Apps     []AppCatalogSummary
	Complete bool
}

// AppCatalog is the read-only, tenant-scoped app surface used by ordinary
// bootstrap requests. Tenant identity comes from Config, never an
// administrator principal or caller-controlled protobuf field.
type AppCatalog interface {
	ListActiveApps(context.Context, string, uint32) (AppCatalogResult, error)
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

// SearchHistory is the immutable, owner-scoped terminal-search metadata
// surface exposed to the browser API. searchhistory.Store satisfies this
// interface directly; recording and retention maintenance remain runtime
// responsibilities rather than browser operations.
type SearchHistory interface {
	Get(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error)
	List(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error)
	Delete(context.Context, searchhistory.AccessScope, string) error
	Clear(context.Context, searchhistory.AccessScope, searchhistory.Filter) (uint64, error)
}

// Exports is the scoped export-job and one-time artifact capability surface.
// export.Manager satisfies this interface directly.
type Exports interface {
	Create(context.Context, searchjobs.AccessScope, exportjobs.CreateRequest) (exportjobs.Job, error)
	Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error)
	List(context.Context, searchjobs.AccessScope, exportjobs.ListRequest) (exportjobs.ListPage, error)
	Cancel(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error)
	CreateDownloadGrant(context.Context, searchjobs.AccessScope, string) (exportjobs.DownloadGrant, error)
	RedeemDownload(context.Context, string) (exportjobs.ArtifactDownload, error)
}

// SearchTimelines is the bounded, owner-scoped on-demand analysis surface.
// The maximum is read when the handler is constructed so request validation
// and the service's enforced limit cannot silently drift within one handler.
type SearchTimelines interface {
	Get(context.Context, searchjobs.AccessScope, searchanalysis.Request) (searchanalysis.Result, error)
	MaximumBuckets() uint32
}

// SearchInspections is the administrator-only, owner-scoped diagnostic
// surface. Tenant and owner identity come from the authenticated browser
// principal rather than from caller-controlled protobuf fields.
type SearchInspections interface {
	Inspect(context.Context, searchjobs.AccessScope, searchinspection.Request) (searchinspection.Result, error)
}

// SearchFields is the bounded, owner-scoped field-catalog surface. The
// handler snapshots both enforced limits during construction so transport
// validation cannot drift from the service contract while requests are live.
type SearchFields interface {
	ListFields(context.Context, searchjobs.AccessScope, searchanalysis.ListFieldsRequest) (searchanalysis.FieldPage, error)
	GetFieldSummary(context.Context, searchjobs.AccessScope, searchanalysis.GetFieldSummaryRequest) (searchanalysis.FieldSummary, error)
	MaximumFields() uint32
	MaximumPageSize() uint32
	MaximumSummaryValues() uint32
}

// IndexFields is the bounded administrator field-catalog surface for one
// already-resolved logical index and immutable storage snapshot. The handler
// snapshots both enforced limits during construction so transport validation
// cannot drift from the service contract while requests are live.
type IndexFields interface {
	ListIndexFields(
		context.Context,
		searchjobs.AccessScope,
		searchanalysis.ListIndexFieldsRequest,
	) (searchanalysis.FieldPage, error)
	MaximumFields() uint32
	MaximumPageSize() uint32
}

// SearchSuggestions is the bounded, no-job SPL editor-completion surface. The
// handler snapshots the enforced maximum during construction so transport
// validation cannot drift while a request is live.
type SearchSuggestions interface {
	Suggest(context.Context, searchsuggestions.Request) (searchsuggestions.Result, error)
	MaximumSuggestions() uint32
}

// SearchWebSocket is the independently lifecycle-managed progress transport.
// Its advertised limits are read from the same service that enforces them so
// bootstrap metadata cannot drift from the live route.
type SearchWebSocket interface {
	http.Handler
	MaximumSubscriptions() uint32
	MaximumPreviewRows() uint32
	MaximumFrameBytes() uint64
	// Close must stop admission and hard-close every upgraded connection before
	// returning, even when ctx expires. An error may report that graceful close
	// timed out, but no handler may remain dependent on search/export services.
	Close(context.Context) error
}

// Handler owns the browser HTTP surface and the exact long-lived WebSocket
// service routed through it. Close therefore cannot accidentally target a
// different service than ServeHTTP upgraded.
type Handler struct {
	next            http.Handler
	searchWebSocket SearchWebSocket
}

func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if handler == nil || handler.next == nil {
		http.NotFound(response, request)
		return
	}
	handler.next.ServeHTTP(response, request)
}

// Close terminates every upgraded progress connection. Ordinary HTTP
// connection shutdown remains owned by cmd/open-splunk-server.
func (handler *Handler) Close(ctx context.Context) error {
	if handler == nil || handler.searchWebSocket == nil {
		return nil
	}
	return handler.searchWebSocket.Close(ctx)
}

// BootstrapConfig contains build information and optional static workspace
// summaries. Apps are a compatibility source for embedded/test deployments
// without AppCatalog; configuring both sources is rejected. Index summaries
// are always loaded from IndexCatalog so authorization and UI bootstrap cannot
// drift apart.
type BootstrapConfig struct {
	ServerVersion           string
	APIVersion              string
	SPLCompatibilityVersion string
	Build                   *opensplunkv1.BuildMetadata
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
	RuntimeReadiness           RuntimeReadiness
	Indexes                    IndexCatalog
	IndexAdmin                 IndexAdministration
	IndexStatistics            IndexStatistics
	IndexStatisticsSnapshotter IndexStatisticsSnapshotter
	IndexDataDeletionAdmission IndexDataDeletionAdmission
	IndexDataDeletionWaker     IndexDataDeletionWaker
	IngestionTokens            IngestionTokenAdministration
	AuditEvents                AuditEvents
	SearchAttemptAuditEvents   SearchAttemptAuditEvents
	CollectorAdmin             CollectorAdministration
	AppAdmin                   AppAdministration
	AppCatalog                 AppCatalog
	SavedSearches              SavedSearches
	SearchHistory              SearchHistory
	Exports                    Exports
	SearchTimelines            SearchTimelines
	SearchInspections          SearchInspections
	SearchFields               SearchFields
	IndexFields                IndexFields
	SearchSuggestions          SearchSuggestions
	SearchWebSocket            SearchWebSocket
	BrowserAuthenticator       auth.BrowserAuthenticator
	WebUI                      fs.FS
	Bootstrap                  BootstrapConfig
	OwnerID                    string
	TenantID                   string
	MaximumRequestBytes        int64
	MaximumPageSize            uint32
	MaximumConcurrentRequests  int
	MaximumConcurrentResponses int
	MaximumConcurrentDownloads int
	RouteTimeout               time.Duration
	Now                        func() time.Time
	// AppCursorKey signs administrator app-list continuations across process
	// restarts. It is required when AppAdmin is configured and must be derived
	// from persisted server key material rather than generated at startup.
	AppCursorKey []byte
	// AdministrativeAllowedHosts is retained as a compatibility name, but is
	// the Host/Origin trust boundary for every browser API route. Values are
	// host names or IP literals without paths. Empty defaults to loopback only.
	AdministrativeAllowedHosts []string
}

type apiHandler struct {
	jobs                       SearchJobs
	indexes                    IndexCatalog
	indexAdmin                 IndexAdministration
	indexStatistics            IndexStatistics
	indexStatisticsSnapshotter IndexStatisticsSnapshotter
	indexDataDeletionAdmission IndexDataDeletionAdmission
	indexDataDeletionWaker     IndexDataDeletionWaker
	ingestionTokens            IngestionTokenAdministration
	auditEvents                AuditEvents
	searchAttemptAuditEvents   SearchAttemptAuditEvents
	collectorAdmin             CollectorAdministration
	appAdmin                   AppAdministration
	appCatalog                 AppCatalog
	savedSearches              SavedSearches
	searchHistory              SearchHistory
	exports                    Exports
	searchTimelines            SearchTimelines
	searchInspections          SearchInspections
	searchFields               SearchFields
	indexFields                IndexFields
	searchSuggestions          SearchSuggestions
	searchWebSocket            SearchWebSocket
	browserAuthenticator       auth.BrowserAuthenticator
	administratorRoutes        map[string]struct{}
	ownerID                    string
	tenantID                   string
	maximumPageSize            uint32
	maximumTimelineBuckets     uint32
	maximumFieldPageSize       uint32
	maximumFieldCatalogFields  uint32
	maximumFieldSummaryValues  uint32
	maxIndexFieldPageSize      uint32
	maxIndexFieldCatalogFields uint32
	maximumSuggestions         uint32
	routeTimeout               time.Duration
	bootstrap                  BootstrapConfig
	now                        func() time.Time
	requestGate                chan struct{}
	serializationGate          chan struct{}
	downloadGate               chan struct{}
	adminCursorKey             [32]byte
	appCursorKey               []byte
	browserAllowedHosts        map[string]struct{}
}

// NewHandler constructs the complete HTTP handler. API paths are dispatched
// before the SPA handler, including unknown API paths, so frontend fallback can
// never conceal an unavailable or misspelled backend route.
func NewHandler(config Config) (*Handler, error) {
	if isNilDependency(config.SearchJobs) {
		return nil, errors.New("create server handler: search job service is required")
	}
	if isNilDependency(config.Indexes) {
		return nil, errors.New("create server handler: index catalog is required")
	}
	runtimeReadiness := config.RuntimeReadiness
	if isNilDependency(runtimeReadiness) {
		runtimeReadiness = nil
	}
	indexAdmin := config.IndexAdmin
	if isNilDependency(indexAdmin) {
		if inferred, ok := config.Indexes.(IndexAdministration); ok && !isNilDependency(inferred) {
			indexAdmin = inferred
		} else {
			indexAdmin = nil
		}
	}
	indexStatistics := config.IndexStatistics
	if isNilDependency(indexStatistics) {
		indexStatistics = nil
	}
	indexStatisticsSnapshotter := config.IndexStatisticsSnapshotter
	if isNilDependency(indexStatisticsSnapshotter) {
		indexStatisticsSnapshotter = nil
	}
	if (indexStatistics == nil) != (indexStatisticsSnapshotter == nil) {
		return nil, errors.New(
			"create server handler: index statistics and snapshotter must be configured together",
		)
	}
	if indexStatistics != nil && indexAdmin == nil {
		return nil, errors.New(
			"create server handler: index statistics requires index administration",
		)
	}
	indexFields := config.IndexFields
	if isNilDependency(indexFields) {
		indexFields = nil
	}
	if indexFields != nil && indexAdmin == nil {
		return nil, errors.New(
			"create server handler: index fields require index administration",
		)
	}
	indexDataDeletionAdmission := config.IndexDataDeletionAdmission
	if isNilDependency(indexDataDeletionAdmission) {
		indexDataDeletionAdmission = nil
	}
	indexDataDeletionWaker := config.IndexDataDeletionWaker
	if isNilDependency(indexDataDeletionWaker) {
		indexDataDeletionWaker = nil
	}
	if (indexDataDeletionAdmission == nil) !=
		(indexDataDeletionWaker == nil) {
		return nil, errors.New(
			"create server handler: index data deletion admission and waker must be configured together",
		)
	}
	if indexDataDeletionAdmission != nil && indexAdmin == nil {
		return nil, errors.New(
			"create server handler: index data deletion requires index administration",
		)
	}
	ingestionTokens := config.IngestionTokens
	if isNilDependency(ingestionTokens) {
		ingestionTokens = nil
	}
	auditEvents := config.AuditEvents
	if isNilDependency(auditEvents) {
		auditEvents = nil
	}
	searchAttemptAuditEvents := config.SearchAttemptAuditEvents
	if isNilDependency(searchAttemptAuditEvents) {
		searchAttemptAuditEvents = nil
	}
	collectorAdmin := config.CollectorAdmin
	if isNilDependency(collectorAdmin) {
		collectorAdmin = nil
	}
	appAdmin := config.AppAdmin
	if isNilDependency(appAdmin) {
		appAdmin = nil
	}
	appCatalog := config.AppCatalog
	if isNilDependency(appCatalog) {
		appCatalog = nil
	}
	if appCatalog != nil && len(config.Bootstrap.Apps) != 0 {
		return nil, errors.New(
			"create server handler: live app catalog and static bootstrap apps cannot both be configured",
		)
	}
	browserAuthenticator := config.BrowserAuthenticator
	if isNilDependency(browserAuthenticator) {
		browserAuthenticator = nil
	}
	inspectionService := config.SearchInspections
	if isNilDependency(inspectionService) {
		inspectionService = nil
	}
	if (indexAdmin != nil ||
		indexStatistics != nil ||
		indexFields != nil ||
		ingestionTokens != nil ||
		auditEvents != nil ||
		searchAttemptAuditEvents != nil ||
		collectorAdmin != nil ||
		appAdmin != nil ||
		inspectionService != nil) &&
		browserAuthenticator == nil {
		return nil, errors.New(
			"create server handler: administrative services require browser authentication",
		)
	}
	var appCursorKey []byte
	if appAdmin != nil {
		if len(config.AppCursorKey) < 32 || len(config.AppCursorKey) > 1<<10 {
			return nil, errors.New(
				"create server handler: app cursor key must contain between 32 and 1024 bytes",
			)
		}
		appCursorKey = slices.Clone(config.AppCursorKey)
	}
	if isNilDependency(config.SavedSearches) {
		return nil, errors.New("create server handler: saved search service is required")
	}
	searchHistoryService := config.SearchHistory
	if isNilDependency(searchHistoryService) {
		searchHistoryService = nil
	}
	exportService := config.Exports
	if isNilDependency(exportService) {
		exportService = nil
	}
	timelineService := config.SearchTimelines
	if isNilDependency(timelineService) {
		timelineService = nil
	}
	maximumTimelineBuckets := uint32(0)
	if timelineService != nil {
		maximumTimelineBuckets = timelineService.MaximumBuckets()
		if maximumTimelineBuckets == 0 || maximumTimelineBuckets > maximumSearchTimelineBuckets {
			return nil, fmt.Errorf("create server handler: timeline maximum buckets must be between 1 and %d", maximumSearchTimelineBuckets)
		}
	}
	fieldService := config.SearchFields
	if isNilDependency(fieldService) {
		fieldService = nil
	}
	maximumFieldCatalogFields := uint32(0)
	maximumFieldPageSize := uint32(0)
	maximumFieldSummaryValues := uint32(0)
	if fieldService != nil {
		maximumFieldCatalogFields = fieldService.MaximumFields()
		maximumFieldPageSize = fieldService.MaximumPageSize()
		maximumFieldSummaryValues = fieldService.MaximumSummaryValues()
		if maximumFieldCatalogFields == 0 || maximumFieldCatalogFields > clickhouse.MaximumFieldCatalogFields {
			return nil, fmt.Errorf("create server handler: field catalog maximum fields must be between 1 and %d", clickhouse.MaximumFieldCatalogFields)
		}
		if maximumFieldPageSize == 0 || maximumFieldPageSize > maximumFieldCatalogFields || maximumFieldPageSize > maximumSearchFieldPageSize {
			return nil, fmt.Errorf("create server handler: field catalog maximum page size must be between 1 and %d and cannot exceed maximum fields", maximumSearchFieldPageSize)
		}
		if maximumFieldSummaryValues == 0 || maximumFieldSummaryValues > clickhouse.MaximumFieldSummaryValues {
			return nil, fmt.Errorf("create server handler: field summary maximum values must be between 1 and %d", clickhouse.MaximumFieldSummaryValues)
		}
	}
	maxIndexFieldCatalogFields := uint32(0)
	maxIndexFieldPageSize := uint32(0)
	if indexFields != nil {
		maxIndexFieldCatalogFields = indexFields.MaximumFields()
		maxIndexFieldPageSize = indexFields.MaximumPageSize()
		if maxIndexFieldCatalogFields == 0 ||
			maxIndexFieldCatalogFields > clickhouse.MaximumFieldCatalogFields {
			return nil, fmt.Errorf(
				"create server handler: index field catalog maximum fields must be between 1 and %d",
				clickhouse.MaximumFieldCatalogFields,
			)
		}
		if maxIndexFieldPageSize == 0 ||
			maxIndexFieldPageSize > maxIndexFieldCatalogFields ||
			maxIndexFieldPageSize > maximumSearchFieldPageSize {
			return nil, fmt.Errorf(
				"create server handler: index field catalog maximum page size must be between 1 and %d and cannot exceed maximum fields",
				maximumSearchFieldPageSize,
			)
		}
	}
	suggestionService := config.SearchSuggestions
	if isNilDependency(suggestionService) {
		suggestionService = nil
	}
	maximumSuggestions := uint32(0)
	if suggestionService != nil {
		maximumSuggestions = suggestionService.MaximumSuggestions()
		if maximumSuggestions == 0 || maximumSuggestions > uint32(spl.MaximumSuggestionLimit) {
			return nil, fmt.Errorf(
				"create server handler: search suggestion maximum must be between 1 and %d",
				spl.MaximumSuggestionLimit,
			)
		}
	}
	searchWebSocket := config.SearchWebSocket
	if isNilDependency(searchWebSocket) {
		searchWebSocket = nil
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
	if maximumFieldPageSize > pageSize {
		return nil, errors.New("create server handler: field catalog maximum page size cannot exceed browser maximum page size")
	}
	if maxIndexFieldPageSize > pageSize {
		return nil, errors.New("create server handler: index field catalog maximum page size cannot exceed browser maximum page size")
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
	concurrentDownloads := config.MaximumConcurrentDownloads
	if concurrentDownloads < 0 || concurrentDownloads > maximumConcurrentDownloads {
		return nil, fmt.Errorf("create server handler: maximum concurrent downloads must be between 1 and %d", maximumConcurrentDownloads)
	}
	if concurrentDownloads == 0 {
		concurrentDownloads = defaultMaximumConcurrentDownloads
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
	if validateBoundedIdentifier(ownerID, maximumSavedSearchOwnerBytes, false) != nil || validateBoundedIdentifier(tenantID, maximumIdentityBytes, false) != nil {
		return nil, errors.New("create server handler: owner or tenant identity is invalid")
	}
	bootstrap, err := normalizeBootstrap(config.Bootstrap)
	if err != nil {
		return nil, err
	}
	bootstrap.SearchWebSocketPath = ""
	bootstrap.MaximumSubscriptions = 0
	bootstrap.MaximumWebSocketBytes = 0
	bootstrap.MaximumPreviewRows = 0
	if searchWebSocket != nil {
		maximumSubscriptions := searchWebSocket.MaximumSubscriptions()
		maximumPreviewRows := searchWebSocket.MaximumPreviewRows()
		maximumFrameBytes := searchWebSocket.MaximumFrameBytes()
		if maximumSubscriptions == 0 || maximumSubscriptions > maximumWebSocketSubscriptions {
			return nil, fmt.Errorf("create server handler: websocket subscriptions must be between 1 and %d", maximumWebSocketSubscriptions)
		}
		if maximumFrameBytes < minimumWebSocketFrameBytes || maximumFrameBytes > maximumWebSocketFrameBytes {
			return nil, fmt.Errorf("create server handler: websocket frame bytes must be between %d and %d", minimumWebSocketFrameBytes, maximumWebSocketFrameBytes)
		}
		if maximumPreviewRows == 0 || maximumPreviewRows > maximumSearchPreviewRows {
			return nil, fmt.Errorf("create server handler: preview rows must be between 1 and %d", maximumSearchPreviewRows)
		}
		bootstrap.SearchWebSocketPath = searchWebSocketPath
		bootstrap.MaximumSubscriptions = maximumSubscriptions
		bootstrap.MaximumPreviewRows = maximumPreviewRows
		bootstrap.MaximumWebSocketBytes = maximumFrameBytes
	}
	bootstrap.Features = featuresForServices(bootstrap.Features, serviceCapabilities{
		history:        searchHistoryService != nil,
		exports:        exportService != nil,
		timeline:       timelineService != nil,
		collectorAdmin: collectorAdmin != nil,
		indexAdmin: indexAdmin != nil &&
			indexStatistics != nil &&
			indexStatisticsSnapshotter != nil &&
			indexFields != nil &&
			indexDataDeletionAdmission != nil &&
			indexDataDeletionWaker != nil,
		appAdmin:           appAdmin != nil,
		planInspection:     inspectionService != nil,
		auditSearch:        auditEvents != nil,
		searchAttemptAudit: searchAttemptAuditEvents != nil,
		fieldDiscovery:     fieldService != nil,
		previews:           searchWebSocket != nil,
	})
	browserAllowedHosts, err := normalizeBrowserAllowedHosts(config.AdministrativeAllowedHosts)
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
		jobs:                       config.SearchJobs,
		indexes:                    config.Indexes,
		indexAdmin:                 indexAdmin,
		indexStatistics:            indexStatistics,
		indexStatisticsSnapshotter: indexStatisticsSnapshotter,
		indexDataDeletionAdmission: indexDataDeletionAdmission,
		indexDataDeletionWaker:     indexDataDeletionWaker,
		ingestionTokens:            ingestionTokens,
		auditEvents:                auditEvents,
		searchAttemptAuditEvents:   searchAttemptAuditEvents,
		collectorAdmin:             collectorAdmin,
		appAdmin:                   appAdmin,
		appCatalog:                 appCatalog,
		savedSearches:              config.SavedSearches,
		searchHistory:              searchHistoryService,
		exports:                    exportService,
		searchTimelines:            timelineService,
		searchInspections:          inspectionService,
		searchFields:               fieldService,
		indexFields:                indexFields,
		searchSuggestions:          suggestionService,
		searchWebSocket:            searchWebSocket,
		browserAuthenticator:       browserAuthenticator,
		ownerID:                    ownerID,
		tenantID:                   tenantID,
		maximumPageSize:            pageSize,
		maximumTimelineBuckets:     maximumTimelineBuckets,
		maximumFieldPageSize:       maximumFieldPageSize,
		maximumFieldCatalogFields:  maximumFieldCatalogFields,
		maximumFieldSummaryValues:  maximumFieldSummaryValues,
		maxIndexFieldPageSize:      maxIndexFieldPageSize,
		maxIndexFieldCatalogFields: maxIndexFieldCatalogFields,
		maximumSuggestions:         maximumSuggestions,
		routeTimeout:               routeTimeout,
		bootstrap:                  bootstrap,
		now:                        now,
		requestGate:                make(chan struct{}, concurrentRequests),
		serializationGate:          make(chan struct{}, concurrentResponses),
		downloadGate:               make(chan struct{}, concurrentDownloads),
		adminCursorKey:             adminCursorKey,
		appCursorKey:               appCursorKey,
		browserAllowedHosts:        browserAllowedHosts,
	}
	apiRouter := api.newRouter(requestBytes, routeTimeout)
	apiRoutes := postAPIRoutes(
		"/api/v1/system/bootstrap",
		"/api/v1/search/validate",
		"/api/v1/search/jobs/create",
		"/api/v1/search/jobs/get",
		searchJobsListPath,
		"/api/v1/search/jobs/results",
		"/api/v1/search/jobs/cancel",
		"/api/v1/saved-searches/create",
		"/api/v1/saved-searches/get",
		"/api/v1/saved-searches/list",
		"/api/v1/saved-searches/update",
		"/api/v1/saved-searches/duplicate",
		"/api/v1/saved-searches/delete",
	)
	administratorRoutes := make(map[string]struct{}, 15)
	if api.searchHistory != nil {
		for _, path := range []string{
			"/api/v1/search/history/get",
			"/api/v1/search/history/list",
			"/api/v1/search/history/delete",
			"/api/v1/search/history/clear",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.indexAdmin != nil {
		for _, path := range []string{
			"/api/v1/indexes/create",
			"/api/v1/indexes/get",
			"/api/v1/indexes/list",
			"/api/v1/indexes/update",
			"/api/v1/indexes/state/set",
			"/api/v1/indexes/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.indexStatistics != nil {
		apiRoutes["/api/v1/indexes/stats/get"] = http.MethodPost
		administratorRoutes["/api/v1/indexes/stats/get"] = struct{}{}
	}
	if api.indexFields != nil {
		apiRoutes[indexFieldsListPath] = http.MethodPost
		administratorRoutes[indexFieldsListPath] = struct{}{}
	}
	if api.ingestionTokens != nil {
		for _, path := range []string{
			"/api/v1/ingestion-tokens/create",
			"/api/v1/ingestion-tokens/get",
			"/api/v1/ingestion-tokens/list",
			"/api/v1/ingestion-tokens/update",
			"/api/v1/ingestion-tokens/revoke",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.auditEvents != nil {
		apiRoutes[auditEventsListPath] = http.MethodPost
		administratorRoutes[auditEventsListPath] = struct{}{}
	}
	if api.searchAttemptAuditEvents != nil {
		apiRoutes[searchAttemptAuditListPath] = http.MethodPost
		administratorRoutes[searchAttemptAuditListPath] = struct{}{}
	}
	if api.collectorAdmin != nil {
		for _, path := range []string{
			"/api/v1/collectors/get",
			"/api/v1/collectors/list",
			"/api/v1/collectors/update",
			"/api/v1/collectors/state/set",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.appAdmin != nil {
		for _, path := range []string{
			"/api/v1/apps/create",
			"/api/v1/apps/get",
			"/api/v1/apps/list",
			"/api/v1/apps/update",
			"/api/v1/apps/state/set",
			"/api/v1/apps/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.exports != nil {
		for _, path := range []string{
			"/api/v1/search/exports/create",
			"/api/v1/search/exports/get",
			exportJobsListPath,
			"/api/v1/search/exports/cancel",
		} {
			apiRoutes[path] = http.MethodPost
		}
		apiRoutes[exportDownloadPath] = http.MethodGet
	}
	if api.searchTimelines != nil {
		apiRoutes[searchTimelinePath] = http.MethodPost
	}
	if api.searchInspections != nil {
		apiRoutes[searchInspectionPath] = http.MethodPost
		administratorRoutes[searchInspectionPath] = struct{}{}
	}
	if api.searchFields != nil {
		apiRoutes[searchFieldsListPath] = http.MethodPost
		apiRoutes[searchFieldSummaryPath] = http.MethodPost
	}
	if api.searchSuggestions != nil {
		apiRoutes[searchSuggestionsPath] = http.MethodPost
	}
	if api.searchWebSocket != nil {
		apiRoutes[searchWebSocketPath] = http.MethodGet
	}
	api.administratorRoutes = administratorRoutes
	apiBoundary := exactAPIRoutes(api.protectBrowserAPIRoutes(apiRouter), apiRoutes)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.Header().Set("Cache-Control", "no-store")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.Header().Set("Cache-Control", "no-store")
		if runtimeReadiness == nil {
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte("not ready\n"))
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), runtimeReadinessTimeout)
		defer cancel()
		if err := runtimeReadiness.Ping(ctx); err != nil {
			// Dependency errors can contain addresses and driver internals. The
			// unauthenticated readiness surface exposes only a fixed marker.
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = response.Write([]byte("not ready\n"))
			return
		}
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	// Register both forms. Without the exact /api pattern, a request for /api
	// itself could otherwise reach the SPA's index document.
	mux.Handle("/api", apiBoundary)
	mux.Handle("/api/", apiBoundary)
	mux.Handle("/", spa)
	return &Handler{next: mux, searchWebSocket: searchWebSocket}, nil
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

func postAPIRoutes(paths ...string) map[string]string {
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		result[path] = http.MethodPost
	}
	return result
}

func exactAPIRoutes(next http.Handler, routes map[string]string) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		method, exists := routes[request.URL.Path]
		if !exists {
			writeAPIError(response, http.StatusNotFound, "API route not found")
			return
		}
		if request.Method != method {
			response.Header().Set("Allow", method)
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
	result.APIVersion = strings.TrimSpace(result.APIVersion)
	if result.APIVersion == "" {
		result.APIVersion = "v1"
	}
	result.SPLCompatibilityVersion = strings.TrimSpace(result.SPLCompatibilityVersion)
	if result.SPLCompatibilityVersion == "" {
		result.SPLCompatibilityVersion = "tier-1-dev"
	}
	if result.Build != nil {
		clonedBuild, serverVersion, err := buildmetadata.Normalize(result.Build, result.ServerVersion)
		if err != nil {
			if errors.Is(err, buildmetadata.ErrVersionMismatch) {
				return BootstrapConfig{}, errors.New("create server handler: bootstrap server version does not match structured build metadata")
			}
			return BootstrapConfig{}, fmt.Errorf("create server handler: bootstrap build metadata: %w", err)
		}
		result.ServerVersion = serverVersion
		result.Build = clonedBuild
	} else if result.ServerVersion == "" {
		result.ServerVersion = "dev"
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

type serviceCapabilities struct {
	history            bool
	exports            bool
	timeline           bool
	collectorAdmin     bool
	indexAdmin         bool
	appAdmin           bool
	planInspection     bool
	auditSearch        bool
	searchAttemptAudit bool
	fieldDiscovery     bool
	previews           bool
}

func featuresForServices(features []opensplunkv1.ServerFeature, capabilities serviceCapabilities) []opensplunkv1.ServerFeature {
	// Managed features describe complete configured API families, not current
	// dependency health or caller authorization. Partial service compositions
	// remain legal but must not overstate their browser contract.
	managed := []struct {
		feature opensplunkv1.ServerFeature
		enabled bool
	}{
		{opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY, capabilities.history},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_CSV, capabilities.exports},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_EXPORT_JSON_LINES, capabilities.exports},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_TIMELINE, capabilities.timeline},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN, capabilities.collectorAdmin},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_INDEX_ADMIN, capabilities.indexAdmin},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_APP_ADMIN, capabilities.appAdmin},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION, capabilities.planInspection},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_AUDIT_SEARCH, capabilities.auditSearch},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT, capabilities.searchAttemptAudit},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY, capabilities.fieldDiscovery},
		{opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_PREVIEW, capabilities.previews},
	}
	enabled := make(map[opensplunkv1.ServerFeature]bool, len(managed))
	for _, item := range managed {
		enabled[item.feature] = item.enabled
	}
	result := make([]opensplunkv1.ServerFeature, 0, len(features)+len(managed))
	seen := make(map[opensplunkv1.ServerFeature]struct{}, len(managed))
	for _, feature := range features {
		if available, controlled := enabled[feature]; controlled {
			if available {
				if _, duplicate := seen[feature]; !duplicate {
					result = append(result, feature)
					seen[feature] = struct{}{}
				}
			}
			continue
		}
		result = append(result, feature)
	}
	for _, item := range managed {
		if _, present := seen[item.feature]; item.enabled && !present {
			result = append(result, item.feature)
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

	routes := []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse](router.RouteConfig[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse]{
			Path: "/system/bootstrap", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse](), Handler: handler.getSystemBootstrap,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.GetSystemBootstrapRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ValidateSearchRequest, *opensplunkv1.ValidateSearchResponse](router.RouteConfig[*opensplunkv1.ValidateSearchRequest, *opensplunkv1.ValidateSearchResponse]{
			Path: "/search/validate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.ValidateSearchRequest, *opensplunkv1.ValidateSearchResponse](), Handler: handler.validateSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.ValidateSearchRequest],
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse](router.RouteConfig[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse]{
			Path: "/search/jobs/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateSearchJobRequest, *opensplunkv1.CreateSearchJobResponse](), Handler: handler.createSearchJob,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.CreateSearchJobRequest],
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse](router.RouteConfig[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse]{
			Path: "/search/jobs/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSearchJobRequest, *opensplunkv1.GetSearchJobResponse](), Handler: handler.getSearchJob,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.GetSearchJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ListSearchJobsRequest, *serializedSearchJobListResponse](router.RouteConfig[*opensplunkv1.ListSearchJobsRequest, *serializedSearchJobListResponse]{
			Path: searchJobsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchJobListCodec(), Handler: handler.listSearchJobs,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.ListSearchJobsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.GetSearchResultsRequest, *serializedSearchResultsResponse](router.RouteConfig[*opensplunkv1.GetSearchResultsRequest, *serializedSearchResultsResponse]{
			Path: "/search/jobs/results", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchResultsCodec(), Handler: handler.getSearchResults,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.GetSearchResultsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse](router.RouteConfig[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse]{
			Path: "/search/jobs/cancel", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CancelSearchJobRequest, *opensplunkv1.CancelSearchJobResponse](), Handler: handler.cancelSearchJob,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.CancelSearchJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse](router.RouteConfig[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse]{
			Path: "/saved-searches/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.CreateSavedSearchRequest, *opensplunkv1.CreateSavedSearchResponse](), Handler: handler.createSavedSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.CreateSavedSearchRequest],
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse](router.RouteConfig[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse]{
			Path: "/saved-searches/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSavedSearchRequest, *opensplunkv1.GetSavedSearchResponse](), Handler: handler.getSavedSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.GetSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.ListSavedSearchesRequest, *serializedSavedSearchListResponse](router.RouteConfig[*opensplunkv1.ListSavedSearchesRequest, *serializedSavedSearchListResponse]{
			Path: "/saved-searches/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSavedSearchListCodec(), Handler: handler.listSavedSearches,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.ListSavedSearchesRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse](router.RouteConfig[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse]{
			Path: "/saved-searches/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.UpdateSavedSearchRequest, *opensplunkv1.UpdateSavedSearchResponse](), Handler: handler.updateSavedSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.UpdateSavedSearchRequest],
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse](router.RouteConfig[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse]{
			Path: "/saved-searches/duplicate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DuplicateSavedSearchRequest, *opensplunkv1.DuplicateSavedSearchResponse](), Handler: handler.duplicateSavedSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.DuplicateSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse](router.RouteConfig[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse]{
			Path: "/saved-searches/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DeleteSavedSearchRequest, *opensplunkv1.DeleteSavedSearchResponse](), Handler: handler.deleteSavedSearch,
			SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.DeleteSavedSearchRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
	if handler.indexAdmin != nil {
		routes = append(routes, handler.indexAdministrationRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.ingestionTokens != nil {
		routes = append(routes, handler.ingestionTokenRoutes(noAuth, maximumRequestBytes, smallRequestBytes)...)
	}
	if handler.auditEvents != nil {
		routes = append(
			routes,
			handler.auditEventRoutes(noAuth, smallRequestBytes)...,
		)
	}
	if handler.searchAttemptAuditEvents != nil {
		routes = append(
			routes,
			handler.searchAttemptAuditRoutes(noAuth, smallRequestBytes)...,
		)
	}
	if handler.collectorAdmin != nil {
		routes = append(
			routes,
			handler.collectorAdministrationRoutes(noAuth, smallRequestBytes)...,
		)
	}
	if handler.appAdmin != nil {
		routes = append(
			routes,
			handler.appAdministrationRoutes(
				noAuth,
				maximumRequestBytes,
				smallRequestBytes,
			)...,
		)
	}
	if handler.searchHistory != nil {
		routes = append(routes, handler.searchHistoryRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.searchTimelines != nil {
		routes = append(routes, handler.searchTimelineRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.searchInspections != nil {
		routes = append(routes, handler.searchInspectionRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.searchFields != nil {
		routes = append(routes, handler.searchFieldRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.searchSuggestions != nil {
		routes = append(
			routes,
			handler.searchSuggestionRoutes(
				noAuth,
				min(maximumRequestBytes, maximumSearchSuggestionRequestBytes),
			)...,
		)
	}
	if handler.exports != nil {
		routes = append(routes,
			newForwardCompatibleProtoRoute[*opensplunkv1.CreateExportJobRequest, *opensplunkv1.CreateExportJobResponse](router.RouteConfig[*opensplunkv1.CreateExportJobRequest, *opensplunkv1.CreateExportJobResponse]{
				Path: "/search/exports/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunkv1.CreateExportJobRequest, *opensplunkv1.CreateExportJobResponse](), Handler: handler.createExportJob,
				SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.CreateExportJobRequest],
			}),
			newForwardCompatibleProtoRoute[*opensplunkv1.GetExportJobRequest, *opensplunkv1.GetExportJobResponse](router.RouteConfig[*opensplunkv1.GetExportJobRequest, *opensplunkv1.GetExportJobResponse]{
				Path: "/search/exports/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunkv1.GetExportJobRequest, *opensplunkv1.GetExportJobResponse](), Handler: handler.getExportJob,
				SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.GetExportJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			}),
			newForwardCompatibleProtoRoute[*opensplunkv1.ListExportJobsRequest, *serializedExportListResponse](router.RouteConfig[*opensplunkv1.ListExportJobsRequest, *serializedExportListResponse]{
				Path: exportJobsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: newSerializedExportListCodec(), Handler: handler.listExportJobs,
				SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.ListExportJobsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			}),
			newForwardCompatibleProtoRoute[*opensplunkv1.CancelExportJobRequest, *opensplunkv1.CancelExportJobResponse](router.RouteConfig[*opensplunkv1.CancelExportJobRequest, *opensplunkv1.CancelExportJobResponse]{
				Path: "/search/exports/cancel", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunkv1.CancelExportJobRequest, *opensplunkv1.CancelExportJobResponse](), Handler: handler.cancelExportJob,
				SourceType: router.Body, Sanitizer: forwardCompatibleProtoSanitizer[*opensplunkv1.CancelExportJobRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			}),
		)
	}
	subRouters := []router.SubRouterConfig{{
		PathPrefix:  apiV1PathPrefix,
		AuthLevel:   &noAuth,
		Middlewares: []sroutercommon.Middleware{disableAPICaching, protobufMiddleware, requestMiddleware, deadlineMiddleware},
		Routes:      unwrapProtobufRoutes(routes),
	}}
	if handler.exports != nil {
		subRouters = append(subRouters, router.SubRouterConfig{
			PathPrefix:  "/api/v1",
			AuthLevel:   &noAuth,
			Middlewares: []sroutercommon.Middleware{disableAPICaching, handler.boundDownloads},
			Routes: []router.RouteDefinition{router.RouteConfigBase{
				Path:           "/search/exports/download",
				Methods:        []router.HttpMethod{router.MethodGet},
				AuthLevel:      &noAuth,
				DisableTimeout: true,
				Handler:        handler.downloadExport,
			}},
		})
	}
	if handler.searchWebSocket != nil {
		subRouters = append(subRouters, router.SubRouterConfig{
			PathPrefix:  "/api/v1",
			AuthLevel:   &noAuth,
			Middlewares: []sroutercommon.Middleware{disableAPICaching},
			Routes: []router.RouteDefinition{router.RouteConfigBase{
				Path:           "/search/ws",
				Methods:        []router.HttpMethod{router.MethodGet},
				AuthLevel:      &noAuth,
				DisableTimeout: true,
				Handler:        handler.searchWebSocket.ServeHTTP,
			}},
		})
	}

	return router.NewRouter[string, struct{}](router.RouterConfig{
		ServiceName: "open-splunk-server",
		// SRouter's built-in timeout returns while its handler goroutine may
		// continue using services. Keep it disabled and apply a synchronous
		// context deadline so http.Server.Shutdown owns every handler lifetime.
		GlobalTimeout:     0,
		GlobalMaxBodySize: maximumRequestBytes,
		SubRouters:        subRouters,
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

func (handler *apiHandler) boundDownloads(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		select {
		case handler.downloadGate <- struct{}{}:
			defer func() { <-handler.downloadGate }()
			next.ServeHTTP(response, request)
		default:
			writeBusyResponse(response, "download request capacity is exhausted")
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
type serializedSearchResultsResponse = boundedProtoResponse[*opensplunkv1.GetSearchResultsResponse]

type serializedSearchResultsCodec = boundedProtoCodec[*opensplunkv1.GetSearchResultsRequest, *opensplunkv1.GetSearchResultsResponse]

func newSerializedSearchResultsCodec() *serializedSearchResultsCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.GetSearchResultsRequest, *opensplunkv1.GetSearchResultsResponse](),
		boundedProtoCodecOptions{
			stateError:   "search result serialization permit is missing",
			messageError: "search result response is missing",
		},
	)
}
