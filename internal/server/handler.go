package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildmetadata"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/dashboards"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"github.com/Suhaibinator/open-splunk/internal/searchsuggestions"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	apiPathPrefix                     = "/api"
	searchJobsListRoute               = "/search/jobs/list"
	searchJobsListPath                = apiPathPrefix + searchJobsListRoute
	exportJobsListRoute               = "/search/exports/list"
	exportJobsListPath                = apiPathPrefix + exportJobsListRoute
	searchFieldsListRoute             = "/search/jobs/fields/list"
	searchFieldsListPath              = apiPathPrefix + searchFieldsListRoute
	indexFieldsListRoute              = "/indexes/fields/list"
	indexFieldsListPath               = apiPathPrefix + indexFieldsListRoute
	searchFieldSummaryRoute           = "/search/jobs/field-summary"
	searchFieldSummaryPath            = apiPathPrefix + searchFieldSummaryRoute
	searchTimelinePath                = "/api/search/jobs/timeline"
	searchInspectionRoute             = "/search/jobs/inspect"
	searchInspectionPath              = apiPathPrefix + searchInspectionRoute
	auditEventsListRoute              = "/audit/events/list"
	auditEventsListPath               = apiPathPrefix + auditEventsListRoute
	searchWebSocketPath               = "/api/search/ws"
	defaultMaximumRequestBytes        = int64(128 << 10)
	defaultMaximumPageSize            = uint32(1_000)
	defaultMaximumConcurrentRequests  = 64
	defaultMaximumConcurrentResponses = 8
	defaultMaximumConcurrentDownloads = 16
	defaultRouteTimeout               = 15 * time.Second
	defaultSearchTimeout              = 2 * time.Minute
	defaultResultRetention            = searchretention.ManualLifetime
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

var (
	ErrTrustedSearchAppUnavailable       = errors.New("trusted search app is unavailable")
	ErrTrustedSearchIndexUnavailable     = errors.New("trusted search index is unavailable")
	ErrTrustedSearchAuthorityUnavailable = errors.New("trusted search authority is unavailable")
)

// TrustedSearchAdmissionRequest is fully parsed search intent. The trusted
// service still re-resolves live app/index authority before creating the job.
type TrustedSearchAdmissionRequest struct {
	SPL               string
	OwnerID           string
	TenantID          string
	AppID             string
	IndexScope        []string
	TimeRange         searchtime.Range
	Source            searchjobs.JobSource
	RetentionLifetime time.Duration
}

// TrustedSearchAdmission is shared by interactive, report, and alert paths in
// production so none of them can drift around current app/index authority.
type TrustedSearchAdmission interface {
	AdmitTrustedSearch(context.Context, TrustedSearchAdmissionRequest) (searchjobs.Job, error)
}

// SearchArtifacts is the durable retained-result surface. It is deliberately
// separate from the live manager: process restarts discard execution state but
// must not invalidate completed job links.
type SearchArtifacts interface {
	Get(context.Context, searchjobs.AccessScope, string, searchartifacts.AccessMode) (searchartifacts.Record, error)
	ListPage(context.Context, searchjobs.AccessScope, searchartifacts.ListRequest) (searchartifacts.ListPage, error)
	Acquire(context.Context, searchjobs.AccessScope, string) (searchartifacts.ResultLease, error)
	ShareExpected(context.Context, searchjobs.AccessScope, string, uint64) (searchartifacts.Record, error)
	UpdateSettingsExpected(context.Context, searchjobs.AccessScope, string, searchartifacts.Settings, uint64) (searchartifacts.Record, error)
}

// knowledgeSearchAdmission reports whether the configured search service will
// resolve and seal knowledge for nonempty app-scoped creates. The capability
// is deliberately separate from public feature advertisement: it exists only
// so the transport can establish live app authority before handing a fixed
// process identity to the internal admission boundary.
type knowledgeSearchAdmission interface {
	KnowledgeAdmissionEnabled() bool
}

type lookupSearchAdmission interface {
	LookupAdmissionEnabled() bool
}

func knowledgeSearchAdmissionEnabled(jobs SearchJobs) bool {
	if isNilDependency(jobs) {
		return false
	}
	admission, ok := jobs.(knowledgeSearchAdmission)
	return ok && !isNilDependency(admission) && admission.KnowledgeAdmissionEnabled()
}

func lookupSearchAdmissionEnabled(jobs SearchJobs) bool {
	if isNilDependency(jobs) {
		return false
	}
	admission, ok := jobs.(lookupSearchAdmission)
	return ok && !isNilDependency(admission) && admission.LookupAdmissionEnabled()
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

// HECOperationalSnapshot is the fixed-shape, administrator-only HEC
// projection. It deliberately has no string, byte, map, or slice fields which
// could carry token, channel, index, request, or event identity.
type HECFixedHistogramSnapshot struct {
	UpperBounds  [13]uint64
	BucketCounts [14]uint64
	Count        uint64
	Sum          uint64
	Max          uint64
}

type HECOperationalSnapshot struct {
	ObservedAt                time.Time
	Requests                  uint64
	Events                    uint64
	UncompressedBytes         uint64
	AuthenticationFailures    uint64
	DecodeFailures            uint64
	EventPolicyFailures       uint64
	AcceptedRequests          uint64
	RateLimitedRequests       uint64
	StagingFailures           uint64
	StagingDuration           time.Duration
	PendingOutboxReservations uint64
	PendingOutboxBytes        uint64
	PendingMetadataBytes      uint64
	PendingUngrouped          uint64
	ReadyWriteGroups          uint64
	AmbiguousWriteGroups      uint64
	LiveWriteGroupLeases      uint64
	OldestPendingOutboxAge    time.Duration
	RequestCapacityAvailable  bool
	RetainedRequests          uint64
	QueueAvailable            bool
	ReconciliationAvailable   bool
	ReconciliationSuccesses   uint64
	ReconciliationRetries     uint64
	ReconciliationAmbiguities uint64
	StagedLogicalBatches      uint64
	StagedLogicalRows         uint64
	FormedWriteGroups         uint64
	PhysicalInsertSends       uint64
	SuccessfulWriteGroups     uint64
	WriteGroupMemberBatches   uint64
	WriteGroupRows            uint64
	WriteGroupDecodedBytes    uint64
	WriteGroupMonthlyParts    uint64
	MemberBatchesPerGroup     HECFixedHistogramSnapshot
	RowsPerGroup              HECFixedHistogramSnapshot
	DecodedBytesPerGroup      HECFixedHistogramSnapshot
	MonthlyPartitionsPerGroup HECFixedHistogramSnapshot
	RowsPerPhysicalInsert     HECFixedHistogramSnapshot
	FillRowTarget             uint64
	FillByteTarget            uint64
	FillHardBoundary          uint64
	FillLinger                uint64
	FillDrain                 uint64
	FillRecovery              uint64
	NativeWaiters             uint64
	PeakNativeWaiters         uint64
	NativeWaiterWakeups       uint64
	NativeWaiterCancellations uint64
	NativeTerminalLookups     uint64
	SealLatencyBuckets        [8]uint64
	SendLatencyBuckets        [8]uint64
	CommitLatencyBuckets      [8]uint64
	LatencyUpperBoundsMicros  [7]uint64
	ActiveChannels            uint64
	RetainedChannels          uint64
	PendingAcknowledgments    uint64
	IndexedAcknowledgments    uint64
	ExpiredAcknowledgments    uint64
	TerminalFailedRequests    uint64
	AcknowledgmentAvailable   bool
	AcknowledgmentQueries     uint64
	AcknowledgmentIDsQueried  uint64
	AcknowledgmentMisses      uint64
	ShutdownRejections        uint64
	ProtocolFailures          [28]uint64
}

// HECOperationalSnapshotter reads one bounded aggregate without returning
// per-token or request-scoped telemetry.
type HECOperationalSnapshotter interface {
	HECOperationalSnapshot(context.Context) (HECOperationalSnapshot, error)
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
	SetCollectorTokenEnabled(context.Context, string, uint64, bool) (auth.CollectorToken, error)
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
	Create(context.Context, savedobjects.AccessScope, *opensplunk.SavedSearchDefinition) (*opensplunk.SavedSearch, error)
	Get(context.Context, savedobjects.AccessScope, string) (*opensplunk.SavedSearch, error)
	List(context.Context, savedobjects.AccessScope, savedobjects.ListRequest) (savedobjects.ListResult, error)
	Update(context.Context, savedobjects.AccessScope, string, uint64, *opensplunk.SavedSearchDefinition, *fieldmaskpb.FieldMask) (*opensplunk.SavedSearch, error)
	Duplicate(context.Context, savedobjects.AccessScope, string, string, *string) (*opensplunk.SavedSearch, error)
	Delete(context.Context, savedobjects.AccessScope, string, uint64) error
}

// Dashboards is the bounded, owner-scoped persisted dashboard surface. Panel
// execution remains in the HTTP service so the stored search definition is
// resolved and admitted through the same path as every other search job.
type Dashboards interface {
	Create(context.Context, dashboards.AccessScope, *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error)
	Get(context.Context, dashboards.AccessScope, string) (*opensplunk.Dashboard, error)
	List(context.Context, dashboards.AccessScope, *string) ([]*opensplunk.Dashboard, error)
	Update(context.Context, dashboards.AccessScope, string, uint64, *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error)
	Delete(context.Context, dashboards.AccessScope, string, uint64) error
}

// SearchHistory is the immutable, owner-scoped terminal-search metadata
// surface exposed to the browser API. searchhistory.Store satisfies this
// interface directly; recording and retention maintenance remain runtime
// responsibilities rather than browser operations.
type SearchHistory interface {
	Get(context.Context, searchhistory.AccessScope, string) (*opensplunk.SearchHistoryEntry, error)
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
	Build                 *opensplunk.BuildMetadata
	SearchWebSocketPath   string
	Features              []opensplunk.ServerFeature
	Apps                  []*opensplunk.AppSummary
	SelectedAppID         string
	MaximumPreviewRows    uint32
	MaximumSubscriptions  uint32
	MaximumWebSocketBytes uint64
	MaximumExportRows     uint64
	MaximumExportBytes    uint64
	DefaultSearchTimeout  time.Duration
	SearchResultRetention time.Duration
}

// Settings is the administrator mutation surface and live bootstrap
// view for node-wide search limits.
type Settings interface {
	Get(context.Context) (control.ServerSearchSettings, error)
	Update(context.Context, uint64, searchlimits.Policy) (control.ServerSearchSettings, error)
	Current() control.ServerSearchSettings
}

// AlertCoordinator is the complete scheduled/run-now execution boundary. It
// remains an interface here so transport tests do not need runtime search and
// webhook dependencies.
type AlertCoordinator interface {
	RunNow(context.Context, string, string) (alerts.RunSummary, error)
}

// Config composes the trusted-network browser API and embedded static UI.
// OwnerID and TenantID are fixed process identities for the initial
// single-user release; authentication can replace them without changing the
// search-job ownership boundary.
type Config struct {
	Logger                     *zap.Logger
	SearchJobs                 SearchJobs
	SearchArtifacts            SearchArtifacts
	TrustedSearchAdmission     TrustedSearchAdmission
	RuntimeReadiness           RuntimeReadiness
	Indexes                    IndexCatalog
	IndexAdmin                 IndexAdministration
	IndexStatistics            IndexStatistics
	IndexStatisticsSnapshotter IndexStatisticsSnapshotter
	IndexDataDeletionAdmission IndexDataDeletionAdmission
	IndexDataDeletionWaker     IndexDataDeletionWaker
	IngestionTokens            IngestionTokenAdministration
	HECOperations              HECOperationalSnapshotter
	AuditEvents                AuditEvents
	SearchAttemptAuditEvents   SearchAttemptAuditEvents
	ServerSettings             Settings
	CollectorAdmin             CollectorAdministration
	AppAdmin                   AppAdministration
	AppCatalog                 AppCatalog
	// Knowledge-management routes are registered only for one complete unit
	// backed by the concrete catalog Writer. Public feature advertisement is
	// derived separately from the complete Tier-1 runtime family.
	KnowledgeCatalog  KnowledgeCatalog
	KnowledgeWriter   KnowledgeWriter
	KnowledgeApps     KnowledgeAppCatalog
	KnowledgeAttempts KnowledgeAttemptJournal
	KnowledgePreview  *knowledgepreview.Service
	// LookupManagement is one complete administrator unit; when absent, none
	// of the lookup routes are registered.
	LookupManagement           LookupManagement
	SavedSearches              SavedSearches
	ScheduledReports           *scheduledreports.Service
	AlertService               *alerts.Service
	AlertRepository            alerts.Repository
	AlertDeliverer             alerts.WebhookDeliverer
	AlertCoordinator           AlertCoordinator
	AlertPublicBaseURL         string
	Dashboards                 Dashboards
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
	// TrustForwardedProto allows a plaintext reverse proxy to assert the
	// browser-facing scheme through one X-Forwarded-Proto header. Direct TLS is
	// always authoritative and cannot be downgraded by the forwarded value.
	TrustForwardedProto bool
}

type apiHandler struct {
	logger                     *zap.Logger
	jobs                       SearchJobs
	searchArtifacts            SearchArtifacts
	trustedSearchAdmission     TrustedSearchAdmission
	indexes                    IndexCatalog
	indexAdmin                 IndexAdministration
	indexStatistics            IndexStatistics
	indexStatisticsSnapshotter IndexStatisticsSnapshotter
	indexDataDeletionAdmission IndexDataDeletionAdmission
	indexDataDeletionWaker     IndexDataDeletionWaker
	ingestionTokens            IngestionTokenAdministration
	hecOperations              HECOperationalSnapshotter
	auditEvents                AuditEvents
	searchAttemptAuditEvents   SearchAttemptAuditEvents
	serverSettings             Settings
	collectorAdmin             CollectorAdministration
	appAdmin                   AppAdministration
	appCatalog                 AppCatalog
	knowledgeCatalog           KnowledgeCatalog
	knowledgeWriter            KnowledgeWriter
	knowledgeApps              KnowledgeAppCatalog
	knowledgeAttempts          KnowledgeAttemptJournal
	knowledgePreview           *knowledgepreview.Service
	lookupManagement           LookupManagement
	knowledgeSearchAdmission   bool
	savedSearches              SavedSearches
	scheduledReports           *scheduledreports.Service
	alertService               *alerts.Service
	alertRepository            alerts.Repository
	alertDeliverer             alerts.WebhookDeliverer
	alertCoordinator           AlertCoordinator
	alertPublicBaseURL         string
	alertsEnabled              bool
	dashboards                 Dashboards
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
	knowledgeAttemptGate       chan struct{}
	downloadGate               chan struct{}
	adminCursorKey             [32]byte
	searchArtifactCursorKey    [32]byte
	appCursorKey               []byte
	browserAllowedHosts        map[string]struct{}
	trustForwardedProto        bool
}

// NewHandler constructs the complete HTTP handler. API paths are dispatched
// before the SPA handler, including unknown API paths, so frontend fallback can
// never conceal an unavailable or misspelled backend route.
func NewHandler(config Config) (*Handler, error) {
	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	if isNilDependency(config.SearchJobs) {
		return nil, errors.New("create server handler: search job service is required")
	}
	runtimeReadiness := config.RuntimeReadiness
	if isNilDependency(runtimeReadiness) {
		runtimeReadiness = nil
	}
	trustedSearchAdmission := config.TrustedSearchAdmission
	if isNilDependency(trustedSearchAdmission) {
		trustedSearchAdmission = nil
	}
	indexServices, err := normalizeHandlerIndexServices(config)
	if err != nil {
		return nil, err
	}
	knowledgeServices, err := normalizeHandlerKnowledgeServices(config)
	if err != nil {
		return nil, err
	}
	lookupServices, err := normalizeHandlerLookupServices(config)
	if err != nil {
		return nil, err
	}
	alertServices := normalizeHandlerAlertServices(config)
	inspectionService := config.SearchInspections
	if isNilDependency(inspectionService) {
		inspectionService = nil
	}
	adminServices, err := normalizeHandlerAdministrativeServices(
		config, indexServices, knowledgeServices, lookupServices, alertServices, inspectionService,
	)
	if err != nil {
		return nil, err
	}
	indexAdmin := indexServices.administration
	indexStatistics := indexServices.statistics
	indexStatisticsSnapshotter := indexServices.statisticsSnapshot
	indexFields := indexServices.fields
	indexDataDeletionAdmission := indexServices.deletionAdmission
	indexDataDeletionWaker := indexServices.deletionWaker
	ingestionTokens := adminServices.ingestionTokens
	hecOperations := adminServices.hecOperations
	auditEvents := adminServices.auditEvents
	searchAttemptAuditEvents := adminServices.searchAttemptAuditEvents
	serverSettings := adminServices.serverSettings
	collectorAdmin := adminServices.collectorAdmin
	appAdmin := adminServices.appAdmin
	browserAuthenticator := adminServices.browserAuthenticator
	appCursorKey := adminServices.appCursorKey
	appCatalog := knowledgeServices.appCatalog
	knowledgeCatalog := knowledgeServices.catalog
	knowledgeWriter := knowledgeServices.writer
	knowledgeApps := knowledgeServices.apps
	knowledgeAttempts := knowledgeServices.attempts
	knowledgePreview := knowledgeServices.preview
	knowledgeAdmission := knowledgeServices.admission
	lookupManagement := lookupServices.management
	lookupAdmission := lookupServices.admission
	alertCoordinator := alertServices.coordinator
	completeAlertFamily := alertServices.complete
	if isNilDependency(config.SavedSearches) {
		return nil, errors.New("create server handler: saved search service is required")
	}
	dashboardService := config.Dashboards
	if isNilDependency(dashboardService) {
		dashboardService = nil
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
	maxIndexFieldCatalogFields, maxIndexFieldPageSize, err := indexServices.limits()
	if err != nil {
		return nil, err
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
	searchArtifacts := config.SearchArtifacts
	if isNilDependency(searchArtifacts) {
		searchArtifacts = nil
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
	completeKnowledgeFamily := knowledgeServices.complete(
		inspectionService, searchHistoryService, exportService, timelineService, fieldService, suggestionService,
	)
	completeLookupFamily := completeKnowledgeFamily &&
		lookupManagement != nil && lookupAdmission
	_, knowledgeQuarantineReady := readyKnowledgeQuarantine(knowledgeWriter)
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
		serverSettings:     serverSettings != nil,
		durableJobs:        searchArtifacts != nil,
		scheduledSearches:  config.ScheduledReports != nil,
		alerts:             completeAlertFamily,
		fieldDiscovery:     fieldService != nil,
		previews:           searchWebSocket != nil,
		knowledge:          completeKnowledgeFamily,
		lookups:            completeLookupFamily,
		dashboards:         dashboardService != nil,
		quarantine:         completeKnowledgeFamily && knowledgeQuarantineReady,
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
	if indexAdmin != nil || ingestionTokens != nil || completeAlertFamily {
		if _, err := rand.Read(adminCursorKey[:]); err != nil {
			return nil, errors.New("create server handler: secure randomness unavailable for administrative cursors")
		}
	}
	var searchArtifactCursorKey [32]byte
	if searchArtifacts != nil {
		if _, err := rand.Read(searchArtifactCursorKey[:]); err != nil {
			return nil, errors.New("create server handler: secure randomness unavailable for retained-result cursors")
		}
	}

	api := &apiHandler{
		logger:                     logger,
		jobs:                       config.SearchJobs,
		searchArtifacts:            searchArtifacts,
		trustedSearchAdmission:     trustedSearchAdmission,
		indexes:                    indexServices.catalog,
		indexAdmin:                 indexAdmin,
		indexStatistics:            indexStatistics,
		indexStatisticsSnapshotter: indexStatisticsSnapshotter,
		indexDataDeletionAdmission: indexDataDeletionAdmission,
		indexDataDeletionWaker:     indexDataDeletionWaker,
		ingestionTokens:            ingestionTokens,
		hecOperations:              hecOperations,
		auditEvents:                auditEvents,
		searchAttemptAuditEvents:   searchAttemptAuditEvents,
		serverSettings:             serverSettings,
		collectorAdmin:             collectorAdmin,
		appAdmin:                   appAdmin,
		appCatalog:                 appCatalog,
		knowledgeCatalog:           knowledgeCatalog,
		knowledgeWriter:            knowledgeWriter,
		knowledgeApps:              knowledgeApps,
		knowledgeAttempts:          knowledgeAttempts,
		knowledgePreview:           knowledgePreview,
		lookupManagement:           lookupManagement,
		knowledgeSearchAdmission:   knowledgeAdmission,
		savedSearches:              config.SavedSearches,
		scheduledReports:           config.ScheduledReports,
		alertService:               config.AlertService,
		alertRepository:            config.AlertRepository,
		alertDeliverer:             config.AlertDeliverer,
		alertCoordinator:           alertCoordinator,
		alertPublicBaseURL:         config.AlertPublicBaseURL,
		alertsEnabled:              completeAlertFamily,
		dashboards:                 dashboardService,
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
		knowledgeAttemptGate:       make(chan struct{}, concurrentRequests),
		downloadGate:               make(chan struct{}, concurrentDownloads),
		adminCursorKey:             adminCursorKey,
		searchArtifactCursorKey:    searchArtifactCursorKey,
		appCursorKey:               appCursorKey,
		browserAllowedHosts:        browserAllowedHosts,
		trustForwardedProto:        config.TrustForwardedProto,
	}
	apiRouter := api.newRouter(requestBytes, routeTimeout)
	apiRoutes := postAPIRoutes(
		"/api/system/bootstrap",
		"/api/search/validate",
		"/api/search/jobs/create",
		"/api/search/jobs/get",
		searchJobsListPath,
		"/api/search/jobs/results",
		"/api/search/jobs/cancel",
		"/api/saved-searches/create",
		"/api/saved-searches/get",
		"/api/saved-searches/list",
		"/api/saved-searches/update",
		"/api/saved-searches/duplicate",
		"/api/saved-searches/delete",
	)
	administratorRoutes := make(map[string]struct{}, 25)
	if api.searchArtifacts != nil {
		for _, path := range []string{
			"/api/search/jobs/settings/get",
			"/api/search/jobs/settings/update",
			"/api/search/jobs/share",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.scheduledReports != nil {
		for _, path := range []string{
			"/api/saved-searches/schedule/set",
			"/api/saved-searches/run",
			"/api/saved-searches/runs/list",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.scheduledReports != nil || completeAlertFamily {
		apiRoutes["/api/schedules/validate"] = http.MethodPost
	}
	if completeAlertFamily {
		for _, path := range []string{
			"/api/alerts/create",
			"/api/alerts/get",
			"/api/alerts/list",
			"/api/alerts/update",
			"/api/alerts/state/set",
			"/api/alerts/delete",
			"/api/alerts/run",
			"/api/alerts/webhook/test",
			"/api/alerts/secret/rotate",
			"/api/alerts/runs/list",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.searchHistory != nil {
		for _, path := range []string{
			"/api/search/history/get",
			"/api/search/history/list",
			"/api/search/history/delete",
			"/api/search/history/clear",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.dashboards != nil {
		for _, path := range []string{
			"/api/dashboards/create",
			"/api/dashboards/get",
			"/api/dashboards/list",
			"/api/dashboards/update",
			"/api/dashboards/delete",
			"/api/dashboards/panels/run",
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.indexAdmin != nil {
		for _, path := range []string{
			"/api/indexes/create",
			"/api/indexes/get",
			"/api/indexes/list",
			"/api/indexes/update",
			"/api/indexes/state/set",
			"/api/indexes/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.indexStatistics != nil {
		apiRoutes["/api/indexes/stats/get"] = http.MethodPost
		administratorRoutes["/api/indexes/stats/get"] = struct{}{}
	}
	if api.indexFields != nil {
		apiRoutes[indexFieldsListPath] = http.MethodPost
		administratorRoutes[indexFieldsListPath] = struct{}{}
	}
	if api.ingestionTokens != nil {
		for _, path := range []string{
			"/api/ingestion-tokens/create",
			"/api/ingestion-tokens/get",
			"/api/ingestion-tokens/list",
			"/api/ingestion-tokens/update",
			"/api/ingestion-tokens/state/set",
			"/api/ingestion-tokens/revoke",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.hecOperations != nil {
		apiRoutes[hecOperationsPath] = http.MethodPost
		administratorRoutes[hecOperationsPath] = struct{}{}
	}
	if api.auditEvents != nil {
		apiRoutes[auditEventsListPath] = http.MethodPost
		administratorRoutes[auditEventsListPath] = struct{}{}
	}
	if api.searchAttemptAuditEvents != nil {
		apiRoutes[searchAttemptAuditListPath] = http.MethodPost
		administratorRoutes[searchAttemptAuditListPath] = struct{}{}
	}
	if api.serverSettings != nil {
		for _, path := range []string{"/api/server/settings/get", "/api/server/settings/update"} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.collectorAdmin != nil {
		for _, path := range []string{
			"/api/collectors/get",
			"/api/collectors/list",
			"/api/collectors/update",
			"/api/collectors/state/set",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.appAdmin != nil {
		for _, path := range []string{
			"/api/apps/create",
			"/api/apps/get",
			"/api/apps/list",
			"/api/apps/update",
			"/api/apps/state/set",
			"/api/apps/delete",
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.knowledgeManagementConfigured() {
		for _, path := range []string{
			knowledgeObjectsCreatePath,
			knowledgeObjectsGetPath,
			knowledgeObjectsListPath,
			knowledgeObjectsDependenciesPath,
			knowledgeObjectsDependentsPath,
			knowledgeObjectsValidatePath,
			knowledgeObjectsUpdatePath,
			knowledgeObjectsSetStatePath,
			knowledgeObjectsDeletePath,
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if _, ready := readyKnowledgeQuarantine(api.knowledgeWriter); ready {
		for _, path := range []string{
			knowledgeObjectsQuarantinePreparePath,
			knowledgeObjectsQuarantinePath,
		} {
			apiRoutes[path] = http.MethodPost
		}
	}
	if api.knowledgePreviewConfigured() {
		apiRoutes[knowledgeObjectsPreviewPath] = http.MethodPost
	}
	if api.lookupManagementConfigured() {
		for _, path := range []string{
			lookupCreatePath,
			lookupGetPath,
			lookupListPath,
			lookupReplacePath,
			lookupSetStatePath,
			lookupDeletePath,
			lookupPreviewPath,
		} {
			apiRoutes[path] = http.MethodPost
			administratorRoutes[path] = struct{}{}
		}
	}
	if api.exports != nil {
		for _, path := range []string{
			"/api/search/exports/create",
			"/api/search/exports/get",
			exportJobsListPath,
			"/api/search/exports/cancel",
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
	apiBoundary := exactAPIRoutes(
		api.protectBrowserAPIRoutes(
			api.protectKnowledgeManagementRoutes(apiRouter),
		),
		apiRoutes,
	)

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
	return nilcheck.IsNil(value)
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
	if result.Build != nil {
		clonedBuild, err := buildmetadata.Normalize(result.Build)
		if err != nil {
			return BootstrapConfig{}, fmt.Errorf("create server handler: bootstrap build metadata: %w", err)
		}
		result.Build = clonedBuild
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
		result.Features = []opensplunk.ServerFeature{
			opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
			opensplunk.ServerFeature_SERVER_FEATURE_SAVED_SEARCHES,
		}
	} else {
		result.Features = slices.Clone(result.Features)
	}
	if len(result.Apps) > maximumBootstrapApps {
		return BootstrapConfig{}, fmt.Errorf("create server handler: bootstrap apps cannot exceed %d", maximumBootstrapApps)
	}
	apps := make([]*opensplunk.AppSummary, len(result.Apps))
	for index, app := range result.Apps {
		if app == nil {
			return BootstrapConfig{}, errors.New("create server handler: bootstrap app cannot be nil")
		}
		apps[index] = proto.Clone(app).(*opensplunk.AppSummary)
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
	knowledge          bool
	lookups            bool
	dashboards         bool
	quarantine         bool
	serverSettings     bool
	durableJobs        bool
	scheduledSearches  bool
	alerts             bool
}

func featuresForServices(features []opensplunk.ServerFeature, capabilities serviceCapabilities) []opensplunk.ServerFeature {
	// Managed features describe complete configured API families, not current
	// dependency health or caller authorization. Partial service compositions
	// remain legal but must not overstate their browser contract.
	managed := []struct {
		feature opensplunk.ServerFeature
		enabled bool
	}{
		{opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_HISTORY, capabilities.history},
		{opensplunk.ServerFeature_SERVER_FEATURE_EXPORT_CSV, capabilities.exports},
		{opensplunk.ServerFeature_SERVER_FEATURE_EXPORT_JSON_LINES, capabilities.exports},
		{opensplunk.ServerFeature_SERVER_FEATURE_TIMELINE, capabilities.timeline},
		{opensplunk.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN, capabilities.collectorAdmin},
		{opensplunk.ServerFeature_SERVER_FEATURE_INDEX_ADMIN, capabilities.indexAdmin},
		{opensplunk.ServerFeature_SERVER_FEATURE_APP_ADMIN, capabilities.appAdmin},
		{opensplunk.ServerFeature_SERVER_FEATURE_PLAN_INSPECTION, capabilities.planInspection},
		{opensplunk.ServerFeature_SERVER_FEATURE_AUDIT_SEARCH, capabilities.auditSearch},
		{opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT, capabilities.searchAttemptAudit},
		{opensplunk.ServerFeature_SERVER_FEATURE_FIELD_DISCOVERY, capabilities.fieldDiscovery},
		{opensplunk.ServerFeature_SERVER_FEATURE_SEARCH_PREVIEW, capabilities.previews},
		{opensplunk.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS, capabilities.knowledge},
		{opensplunk.ServerFeature_SERVER_FEATURE_LOOKUP_MANAGEMENT, capabilities.lookups},
		{opensplunk.ServerFeature_SERVER_FEATURE_DASHBOARDS, capabilities.dashboards},
		{opensplunk.ServerFeature_SERVER_FEATURE_KNOWLEDGE_QUARANTINE, capabilities.quarantine},
		{opensplunk.ServerFeature_SERVER_FEATURE_SERVER_SETTINGS_ADMIN, capabilities.serverSettings},
		{opensplunk.ServerFeature_SERVER_FEATURE_DURABLE_SEARCH_JOBS, capabilities.durableJobs},
		{opensplunk.ServerFeature_SERVER_FEATURE_SCHEDULED_SEARCHES, capabilities.scheduledSearches},
		{opensplunk.ServerFeature_SERVER_FEATURE_ALERTS, capabilities.alerts},
	}
	enabled := make(map[opensplunk.ServerFeature]bool, len(managed))
	for _, item := range managed {
		enabled[item.feature] = item.enabled
	}
	result := make([]opensplunk.ServerFeature, 0, len(features)+len(managed))
	seen := make(map[opensplunk.ServerFeature]struct{}, len(managed))
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

const (
	// SRouter logs every HTTPError it renders at Error level, including client
	// faults such as a 404 for an unknown search job, so an unauthenticated
	// scanner can drive one ERROR line per request through the process
	// logger's single output mutex. Sample that traffic: the first
	// srouterLogSampleFirst records in each interval are always emitted, then
	// one in every srouterLogSampleThereafter, per level and message.
	srouterLogSampleInterval   = time.Second
	srouterLogSampleFirst      = 100
	srouterLogSampleThereafter = 100
)

// newSRouterLogger derives the child logger handed to SRouter. Sampling is
// applied here rather than to the process logger so that server and collector
// operational records - startup, shutdown, ingest and query failures, each of
// which is emitted at most a handful of times - are never dropped. The child
// is named so a sampled record is attributable to the HTTP layer. logger must
// be non-nil; NewHandler substitutes a no-op logger for a nil Config.Logger.
func newSRouterLogger(logger *zap.Logger) *zap.Logger {
	return logger.Named("http").WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(
			core,
			srouterLogSampleInterval,
			srouterLogSampleFirst,
			srouterLogSampleThereafter,
		)
	}))
}

func (handler *apiHandler) srouterDependencies() router.RouterDependencies[string, struct{}] {
	dependencies := router.RouterDependencies[string, struct{}]{}
	if handler.bootstrap.Build != nil {
		buildID := handler.bootstrap.Build.GetProductVersion()
		if buildID == "" {
			buildID = handler.bootstrap.Build.GetSourceRevision()
		}
		dependencies.BuildID = func() string { return buildID }
	}
	return dependencies
}

func (handler *apiHandler) newRouter(maximumRequestBytes int64, routeTimeout time.Duration) http.Handler {
	// NewHandler substitutes a no-op logger for a nil Config.Logger, so this is
	// always non-nil.
	routerLogger := newSRouterLogger(handler.logger)
	noAuth := router.NoAuth
	protobufMiddleware := requireProtobufContentType
	requestMiddleware := handler.boundRequests
	deadlineMiddleware := withSynchronousDeadline(routeTimeout)
	smallRequestBytes := min(maximumRequestBytes, maximumSmallRequestBytes)

	routes := []router.RouteDefinition{
		router.RouteConfig[*opensplunk.GetSystemBootstrapRequest, *opensplunk.GetSystemBootstrapResponse]{
			Path: "/system/bootstrap", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetSystemBootstrapRequest, *opensplunk.GetSystemBootstrapResponse](), Handler: handler.getSystemBootstrap,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeGetSystemBootstrapRequest,
		},
		router.RouteConfig[*opensplunk.ValidateSearchRequest, *opensplunk.ValidateSearchResponse]{
			Path: "/search/validate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ValidateSearchRequest, *opensplunk.ValidateSearchResponse](), Handler: handler.validateSearch,
			SourceType: router.Body,
			Sanitizer:  sanitizeValidateSearchRequest,
		},
		router.RouteConfig[*opensplunk.CreateSearchJobRequest, *opensplunk.CreateSearchJobResponse]{
			Path: "/search/jobs/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateSearchJobRequest, *opensplunk.CreateSearchJobResponse](), Handler: handler.createSearchJob,
			SourceType: router.Body,
			Sanitizer:  sanitizeCreateSearchJobRequest,
		},
		router.RouteConfig[*opensplunk.GetSearchJobRequest, *opensplunk.GetSearchJobResponse]{
			Path: "/search/jobs/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetSearchJobRequest, *opensplunk.GetSearchJobResponse](), Handler: handler.getSearchJob,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeGetSearchJobRequest,
		},
		router.RouteConfig[*opensplunk.ListSearchJobsRequest, *serializedSearchJobListResponse]{
			Path: searchJobsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchJobListCodec(), Handler: handler.listSearchJobs,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeListSearchJobsRequest,
		},
		router.RouteConfig[*opensplunk.GetSearchResultsRequest, *serializedSearchResultsResponse]{
			Path: "/search/jobs/results", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchResultsCodec(), Handler: handler.getSearchResults,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeGetSearchResultsRequest,
		},
		router.RouteConfig[*opensplunk.CancelSearchJobRequest, *opensplunk.CancelSearchJobResponse]{
			Path: "/search/jobs/cancel", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CancelSearchJobRequest, *opensplunk.CancelSearchJobResponse](), Handler: handler.cancelSearchJob,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeCancelSearchJobRequest,
		},
		router.RouteConfig[*opensplunk.CreateSavedSearchRequest, *opensplunk.CreateSavedSearchResponse]{
			Path: "/saved-searches/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.CreateSavedSearchRequest, *opensplunk.CreateSavedSearchResponse](), Handler: handler.createSavedSearch,
			SourceType: router.Body,
			Sanitizer:  sanitizeCreateSavedSearchRequest,
		},
		router.RouteConfig[*opensplunk.GetSavedSearchRequest, *opensplunk.GetSavedSearchResponse]{
			Path: "/saved-searches/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetSavedSearchRequest, *opensplunk.GetSavedSearchResponse](), Handler: handler.getSavedSearch,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeGetSavedSearchRequest,
		},
		router.RouteConfig[*opensplunk.ListSavedSearchesRequest, *serializedSavedSearchListResponse]{
			Path: "/saved-searches/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSavedSearchListCodec(), Handler: handler.listSavedSearches,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeListSavedSearchesRequest,
		},
		router.RouteConfig[*opensplunk.UpdateSavedSearchRequest, *opensplunk.UpdateSavedSearchResponse]{
			Path: "/saved-searches/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateSavedSearchRequest, *opensplunk.UpdateSavedSearchResponse](), Handler: handler.updateSavedSearch,
			SourceType: router.Body,
			Sanitizer:  sanitizeUpdateSavedSearchRequest,
		},
		router.RouteConfig[*opensplunk.DuplicateSavedSearchRequest, *opensplunk.DuplicateSavedSearchResponse]{
			Path: "/saved-searches/duplicate", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DuplicateSavedSearchRequest, *opensplunk.DuplicateSavedSearchResponse](), Handler: handler.duplicateSavedSearch,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeDuplicateSavedSearchRequest,
		},
		router.RouteConfig[*opensplunk.DeleteSavedSearchRequest, *opensplunk.DeleteSavedSearchResponse]{
			Path: "/saved-searches/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DeleteSavedSearchRequest, *opensplunk.DeleteSavedSearchResponse](), Handler: handler.deleteSavedSearch,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeDeleteSavedSearchRequest,
		},
	}
	if handler.dashboards != nil {
		routes = append(routes, handler.dashboardRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.searchArtifacts != nil {
		routes = append(routes, handler.searchArtifactRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.scheduledReports != nil {
		routes = append(routes, handler.scheduledReportRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.scheduledReports != nil || handler.alertsEnabled {
		routes = append(routes, handler.scheduleValidationRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.alertsEnabled {
		routes = append(routes, handler.alertRoutes(noAuth, maximumRequestBytes, smallRequestBytes)...)
	}
	if handler.indexAdmin != nil {
		routes = append(routes, handler.indexAdministrationRoutes(noAuth, smallRequestBytes)...)
	}
	if handler.ingestionTokens != nil {
		routes = append(routes, handler.ingestionTokenRoutes(noAuth, maximumRequestBytes, smallRequestBytes)...)
	}
	if handler.hecOperations != nil {
		routes = append(routes, handler.hecOperationalRoutes(noAuth, smallRequestBytes)...)
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
	if handler.serverSettings != nil {
		routes = append(routes, handler.serverSettingsRoutes(noAuth, smallRequestBytes)...)
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
	if handler.knowledgeManagementConfigured() {
		routes = append(
			routes,
			handler.knowledgeManagementRoutes(noAuth)...,
		)
	}
	if handler.lookupManagementConfigured() {
		routes = append(routes, handler.lookupManagementRoutes(noAuth)...)
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
			router.RouteConfig[*opensplunk.CreateExportJobRequest, *opensplunk.CreateExportJobResponse]{
				Path: "/search/exports/create", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunk.CreateExportJobRequest, *opensplunk.CreateExportJobResponse](), Handler: handler.createExportJob,
				SourceType: router.Body,
				Sanitizer:  sanitizeCreateExportJobRequest,
			},
			router.RouteConfig[*opensplunk.GetExportJobRequest, *opensplunk.GetExportJobResponse]{
				Path: "/search/exports/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunk.GetExportJobRequest, *opensplunk.GetExportJobResponse](), Handler: handler.getExportJob,
				SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
				Sanitizer: sanitizeGetExportJobRequest,
			},
			router.RouteConfig[*opensplunk.ListExportJobsRequest, *serializedExportListResponse]{
				Path: exportJobsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: newSerializedExportListCodec(), Handler: handler.listExportJobs,
				SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
				Sanitizer: handler.sanitizeListExportJobsRequest,
			},
			router.RouteConfig[*opensplunk.CancelExportJobRequest, *opensplunk.CancelExportJobResponse]{
				Path: "/search/exports/cancel", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
				Codec: codec.NewProtoCodec[*opensplunk.CancelExportJobRequest, *opensplunk.CancelExportJobResponse](), Handler: handler.cancelExportJob,
				SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
				Sanitizer: sanitizeCancelExportJobRequest,
			},
		)
	}
	apiRouter := router.NewRouter[string, struct{}](router.RouterConfig{
		ServiceName: "open-splunk-server",
		Logger:      routerLogger,
		// SRouter's built-in timeout returns while its handler goroutine may
		// continue using services. Keep it disabled and apply a synchronous
		// context deadline so http.Server.Shutdown owns every handler lifetime.
		GlobalTimeout:     0,
		GlobalMaxBodySize: maximumRequestBytes,
	}, handler.srouterDependencies())
	apiRouter.Group(apiPathPrefix).
		Auth(noAuth).
		Use(disableAPICaching, protobufMiddleware, requestMiddleware, deadlineMiddleware).
		Route(routes...)
	if handler.exports != nil {
		apiRouter.Group("/api").
			Auth(noAuth).
			Use(disableAPICaching, handler.boundDownloads).
			Route(router.RouteConfigBase{
				Path:           "/search/exports/download",
				Methods:        []router.HttpMethod{router.MethodGet},
				AuthLevel:      &noAuth,
				DisableTimeout: true,
				Handler:        handler.downloadExport,
			})
	}
	if handler.searchWebSocket != nil {
		apiRouter.Group("/api").
			Auth(noAuth).
			Use(disableAPICaching).
			Route(router.RouteConfigBase{
				Path:           "/search/ws",
				Methods:        []router.HttpMethod{router.MethodGet},
				AuthLevel:      &noAuth,
				DisableTimeout: true,
				Handler:        handler.searchWebSocket.ServeHTTP,
			})
	}
	return apiRouter
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
type serializedSearchResultsResponse = boundedProtoResponse[*opensplunk.GetSearchResultsResponse]

type serializedSearchResultsCodec = boundedProtoCodec[*opensplunk.GetSearchResultsRequest, *opensplunk.GetSearchResultsResponse]

func newSerializedSearchResultsCodec() *serializedSearchResultsCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.GetSearchResultsRequest, *opensplunk.GetSearchResultsResponse](),
		boundedProtoCodecOptions{
			stateError:   "search result serialization permit is missing",
			messageError: "search result response is missing",
		},
	)
}
