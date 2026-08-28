package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"fortio.org/safecast"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	opensplunkroot "github.com/Suhaibinator/open-splunk"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/dashboards"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/hechttp"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	startupTimeout                     = 2 * time.Minute
	shutdownTimeout                    = 35 * time.Second
	defaultIndexRetention              = ingest.DefaultIndexRetention
	defaultOwnerID                     = "single-user"
	auditCursorKeyPurpose              = "audit-event-cursors"
	searchAttemptAuditCursorKeyPurpose = "search-attempt-audit-cursors"
	collectorHeartbeatFlushInterval    = time.Second
	collectorHeartbeatWriteTimeout     = 5 * time.Second
	collectorHeartbeatReleaseTimeout   = 2 * collectorHeartbeatWriteTimeout
	// SQLite may spend five seconds waiting on a competing writer. Reserve a
	// second write window for transaction overhead and a useful retry after
	// heartbeat Release has consumed its own bound.
	collectorDurableDisconnectReserve = 2 * collectorHeartbeatWriteTimeout
	collectorSessionCleanupTimeout    = collectorHeartbeatReleaseTimeout +
		collectorDurableDisconnectReserve
	collectorShutdownGraceTimeout = shutdownTimeout -
		collectorSessionCleanupTimeout
)

type options struct {
	verifyEmbeddedRelease                     bool
	httpAddress                               string
	httpAllowedHosts                          []string
	httpAllowedHostsCSV                       string
	httpTLSCert                               string
	httpTLSKey                                string
	controlDBPath                             string
	masterKeyPath                             string
	administratorTokenFile                    string
	exportArtifactDir                         string
	clickhouseAddress                         string
	clickhouseDatabase                        string
	clickhouseUsername                        string
	clickhouseSkipMigrations                  bool
	clickhousePasswordFile                    string
	clickhouseSecure                          bool
	clickhouseCACertFile                      string
	clickhouseServerName                      string
	collectorAddress                          string
	collectorInsecure                         bool
	collectorTLSCert                          string
	collectorTLSKey                           string
	hecEnabled                                bool
	indexRetention                            time.Duration
	searchHistoryMaximumAge                   time.Duration
	searchHistoryMaximumEntriesPerOwner       int
	searchAttemptAuditMaximumRetainedAttempts int
	tenantID                                  string
}

type visibilitySnapshotter struct {
	sequencer visibility.Sequencer
}

func (snapshotter visibilitySnapshotter) VisibilityCutoff(ctx context.Context) (uint64, error) {
	return snapshotter.sequencer.Cutoff(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if handled, err := runDeploymentSubcommand(os.Args[1:]); handled {
		return err
	}
	config := parseFlags()
	return runWithOptions(config)
}

func runWithOptions(config options) error {
	if err := normalizeRuntimeOptions(&config); err != nil {
		return err
	}
	collectorTransportConfig := collectorServerConfig{
		Address:     config.collectorAddress,
		Insecure:    config.collectorInsecure,
		TLSCertFile: config.collectorTLSCert,
		TLSKeyFile:  config.collectorTLSKey,
	}
	if err := validateCollectorServerConfig(collectorTransportConfig); err != nil {
		return err
	}
	var clickHouseTLSProfile *clickHouseClientTLSProfile
	if !config.verifyEmbeddedRelease {
		var err error
		clickHouseTLSProfile, err = loadClickHouseClientTLSProfile(
			config.clickhouseSecure,
			config.clickhouseCACertFile,
			config.clickhouseServerName,
		)
		if err != nil {
			return err
		}
	}
	if config.verifyEmbeddedRelease {
		release, err := opensplunkroot.EmbeddedRelease()
		if err != nil {
			return fmt.Errorf("open embedded release: %w", err)
		}
		return writeEmbeddedReleaseVerification(os.Stdout, release)
	}
	httpTLSConfig, err := loadHTTPServerTLSConfig(
		config.httpTLSCert,
		config.httpTLSKey,
	)
	if err != nil {
		return err
	}
	clickHouseOptions, err := configureClickHouseConnectionOptions(
		config,
		clickHouseTLSProfile,
	)
	if err != nil {
		return err
	}
	defer discardClickHouseMigrationCredentials(&clickHouseOptions)
	browserAuthenticator, err := newAdministratorBrowserAuthenticator(
		config.administratorTokenFile,
		config.tenantID,
		defaultOwnerID,
	)
	if err != nil {
		return err
	}
	release, err := opensplunkroot.EmbeddedRelease()
	if err != nil {
		return fmt.Errorf("open embedded release: %w", err)
	}
	buildMetadata := &opensplunk.BuildMetadata{
		SourceRevision:             release.Metadata.SourceRevision,
		UiBuildId:                  release.Metadata.UIBuildID,
		UiSha256:                   release.Metadata.UI.SHA256,
		ProtobufSchemaSha256:       release.Metadata.ProtobufSchema.SHA256,
		SqliteMigrationsSha256:     release.Metadata.SQLiteMigrations.SHA256,
		ClickhouseMigrationsSha256: release.Metadata.ClickHouseMigrations.SHA256,
	}
	if release.Metadata.ProductVersion != "" {
		buildMetadata.ProductVersion = &release.Metadata.ProductVersion
	}
	exportSettings := defaultExportRuntimeSettings()
	if err := exportSettings.validate(); err != nil {
		return fmt.Errorf("validate export runtime: %w", err)
	}
	serverLock, err := acquireServerLock(config.controlDBPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := serverLock.Close(); err != nil {
			log.Printf("release server lock: %v", err)
		}
	}()

	startupContext, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelStartup()

	controlDB, err := control.Open(startupContext, config.controlDBPath)
	if err != nil {
		return fmt.Errorf("open control plane: %w", err)
	}
	defer func() {
		if err := controlDB.Close(); err != nil {
			log.Printf("close control plane: %v", err)
		}
	}()
	collectorBootEpoch, err := newCollectorBootEpoch()
	if err != nil {
		return err
	}
	collectorFleet, err := collectorfleet.New(controlDB)
	if err != nil {
		return fmt.Errorf("open collector fleet: %w", err)
	}
	if _, err := collectorFleet.InvalidatePriorBootLeases(
		startupContext,
		collectorBootEpoch,
		time.Now().UTC(),
	); err != nil {
		return fmt.Errorf("invalidate prior collector leases: %w", err)
	}

	sequencer, err := visibility.NewSQLite(startupContext, controlDB)
	if err != nil {
		return fmt.Errorf("open visibility sequencer: %w", err)
	}
	// The sequencer borrows the control database and exclusively owns its
	// process-local attempt leases. This defer is registered after the
	// control-database defer and before every sequencer consumer, so LIFO
	// shutdown first drains those consumers, then releases durable attempt
	// leases, and only then closes SQLite. If this component exhausts the
	// process shutdown budget, its finalizer remains responsible for
	// unregistering ownership after admitted work exits; closing SQLite then
	// forces any driver work that ignored cancellation to return.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := sequencer.Shutdown(ctx); err != nil {
			log.Printf("shutdown visibility sequencer: %v", err)
		}
	}()
	securityStores, err := openRuntimeSecurityStoresWithSearchAttemptMaximum(
		startupContext,
		controlDB,
		config.masterKeyPath,
		config.tenantID,

		safecast.MustConv[uint32](config.searchAttemptAuditMaximumRetainedAttempts),
	)
	if err != nil {
		return err
	}
	indexAdministration, err := newRuntimeIndexAdministration(
		controlDB,
		config.tenantID,
		securityStores.auditEvents,
	)
	if err != nil {
		return err
	}
	savedSearches := securityStores.savedSearches
	dashboardStore, err := dashboards.New(controlDB, dashboards.Options{TenantID: config.tenantID})
	if err != nil {
		return fmt.Errorf("open dashboard store: %w", err)
	}
	tokenStore := securityStores.ingestionTokens
	collectorAdmissions, err := collectoradmission.New(
		controlDB,
		tokenStore,
		config.tenantID,
	)
	if err != nil {
		return fmt.Errorf("open collector admission coordinator: %w", err)
	}
	searchHistory, err := openSearchHistoryStore(
		startupContext,
		controlDB,
		config.masterKeyPath,
		searchhistory.Options{
			MaximumAge:                config.searchHistoryMaximumAge,
			MaximumEntriesPerOwner:    config.searchHistoryMaximumEntriesPerOwner,
			AuditAppender:             securityStores.searchAttemptAuditEvents,
			RequireSearchAttemptAudit: true,
		},
	)
	if err != nil {
		return err
	}
	searchHistoryScope := searchhistory.AccessScope{
		TenantID: config.tenantID,
		OwnerID:  defaultOwnerID,
	}
	recoveredSearches, err := searchHistory.RecoverInterrupted(
		startupContext,
		searchHistoryScope,
	)
	if err != nil {
		return fmt.Errorf("recover interrupted search history: %w", err)
	}
	if recoveredSearches != 0 {
		log.Printf("recovered %d interrupted search attempts", recoveredSearches)
	}
	startupHistoryPrune, err := pruneSearchHistoryAtStartup(
		startupContext,
		searchHistory,
	)
	if err != nil {
		return err
	}
	if startupHistoryPrune.deleted != 0 {
		log.Printf(
			"pruned %d terminal search-history entries during startup",
			startupHistoryPrune.deleted,
		)
	}
	searchHistoryMaintenanceConfig := defaultSearchHistoryMaintenanceConfig()
	searchHistoryMaintenanceConfig.initialCursor = startupHistoryPrune.cursor
	searchHistoryMaintenanceConfig.runImmediately = startupHistoryPrune.more
	searchHistoryMaintenanceConfig.onError = func(err error) {
		log.Printf("maintain search-history retention: %v", err)
	}
	searchHistoryMaintenance, err := newSearchHistoryMaintenance(
		searchHistory,
		searchHistoryMaintenanceConfig,
	)
	if err != nil {
		return fmt.Errorf("create search-history maintenance: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := searchHistoryMaintenance.Close(ctx); err != nil {
			log.Printf("close search-history maintenance: %v", err)
		}
	}()
	if err := applyConfiguredStartupClickHouseMigrations(
		startupContext,
		config.clickhouseSkipMigrations,
		clickHouseOptions.migration,
		migrations.ClickHouse(),
		func(
			options *clickhousedriver.Options,
		) (clickHouseMigrationSession, error) {
			return openClickHouse(options)
		},
		server.ApplyClickHouseMigrations,
	); err != nil {
		return err
	}
	discardClickHouseMigrationCredentials(&clickHouseOptions)

	connection, err := openClickHouse(clickHouseOptions.runtime)
	if err != nil {
		return err
	}
	var deletionConnection clickhousedriver.Conn
	var eventStore *internalclickhouse.Store
	var indexDataDeletion *indexDataDeletionRuntime
	defer func() {
		// A successful runtime owner closes both Store connections. Before
		// ownership transfers, this guard unwinds partial startup in reverse
		// privilege order.
		if indexDataDeletion != nil {
			return
		}
		if eventStore != nil {
			if err := eventStore.Close(); err != nil {
				log.Printf("close ClickHouse Store after failed startup: %v", err)
			}
			return
		}
		if deletionConnection != nil {
			if err := deletionConnection.Close(); err != nil {
				log.Printf(
					"close ClickHouse deletion session after failed startup: %v",
					err,
				)
			}
		}
		if err := connection.Close(); err != nil {
			log.Printf("close ClickHouse runtime session after failed startup: %v", err)
		}
	}()
	if err := connection.Ping(startupContext); err != nil {
		return fmt.Errorf("ping ClickHouse runtime session: %w", err)
	}
	if err := server.ValidateClickHouseApplicationPrivileges(
		startupContext,
		connection,
	); err != nil {
		return err
	}
	deletionConnection, err = openClickHouse(clickHouseOptions.deletion)
	if err != nil {
		return err
	}
	if err := deletionConnection.Ping(startupContext); err != nil {
		return fmt.Errorf("ping ClickHouse deletion session: %w", err)
	}
	if err := server.ValidateClickHouseApplicationPrivileges(
		startupContext,
		deletionConnection,
	); err != nil {
		return err
	}

	eventStore, err = internalclickhouse.NewStoreWithDeletionConnection(
		connection,
		deletionConnection,
		controlRetentionProvider{
			catalog: controlDB, tenantID: config.tenantID, defaultRetention: config.indexRetention,
		},
		sequencer,
	)
	if err != nil {
		return fmt.Errorf("create ClickHouse ingestion store: %w", err)
	}
	indexReads, err := newIndexReadLifecycle(
		controlDB,
		config.tenantID,
	)
	if err != nil {
		return err
	}
	indexDataDeletion, err = newIndexDataDeletionRuntime(
		controlDB,
		eventStore,
		indexReads.retirement,
		config.tenantID,
		func(err error) {
			log.Printf("reconcile index data deletion: %v", err)
		},
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := finalizeIndexDataDeletionRuntime(
			indexDataDeletion,
			shutdownTimeout,
		); err != nil {
			log.Printf("close index data deletion runtime: %v", err)
		}
	}()
	var hecTerminalCleanup *hecTerminalMaintenance
	if config.hecEnabled {
		maintenanceConfig := defaultHECTerminalMaintenanceConfig()
		startupPrune, pruneErr := runHECTerminalPruneBatches(
			startupContext,
			sequencer,
			time.Now().Round(0).UTC().Add(-maintenanceConfig.retention),
			maintenanceConfig.batchSize,
			maintenanceConfig.maximumBatches,
		)
		if pruneErr != nil {
			return fmt.Errorf("prune expired HEC terminal requests at startup: %w", pruneErr)
		}
		if startupPrune.deleted != 0 {
			log.Printf("pruned %d expired HEC terminal requests during startup", startupPrune.deleted)
		}
		maintenanceConfig.runImmediately = startupPrune.more
		maintenanceConfig.onError = func(err error) {
			log.Printf("maintain HEC terminal retention: %v", err)
		}
		hecTerminalCleanup, err = newHECTerminalMaintenance(sequencer, maintenanceConfig)
		if err != nil {
			return fmt.Errorf("create HEC terminal maintenance: %w", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			if err := hecTerminalCleanup.Close(ctx); err != nil {
				log.Printf("close HEC terminal maintenance: %v", err)
			}
		}()
	}
	var collectorHeartbeats *collectorfleet.HeartbeatRuntime
	var ingestService opensplunk.CollectorIngestServiceServer
	if strings.TrimSpace(config.collectorAddress) != "" {
		ingestConfig := ingest.DefaultConfig()
		ingestConfig.SessionCleanupTimeout = collectorSessionCleanupTimeout
		collectorHeartbeats, err = collectorfleet.NewHeartbeatRuntime(
			collectorFleet,
			collectorfleet.HeartbeatRuntimeConfig{
				MaxCollectors:     collectorMaxActiveStreams,
				HeartbeatInterval: ingestConfig.HeartbeatInterval,
				StaleGrace:        2 * ingestConfig.HeartbeatInterval,
				FlushInterval:     collectorHeartbeatFlushInterval,
				WriteTimeout:      collectorHeartbeatWriteTimeout,
				MonotonicNow:      time.Now,
				OnError: func(err error) {
					log.Printf("persist collector heartbeat: %v", err)
				},
			},
		)
		if err != nil {
			return fmt.Errorf("create collector heartbeat runtime: %w", err)
		}
		defer func() {
			if collectorHeartbeats == nil {
				return
			}
			if err := closeCollectorHeartbeatRuntime(
				collectorHeartbeats,
				shutdownTimeout,
			); err != nil {
				log.Printf("close collector heartbeat runtime after failed startup: %v", err)
			}
		}()
		ingestConfig.Build = buildMetadata
		ingestConfig.ServerInstanceID = collectorBootEpoch
		ingestConfig.DefaultIndexRetention = config.indexRetention
		ingestConfig.SessionManager = collectorSessionManager{
			admission:  collectorAdmissions,
			fleet:      collectorFleet,
			heartbeats: collectorHeartbeats,
		}
		ingestConfig.SessionErrorHandler = func(err error) {
			log.Printf("collector session cleanup: %v", err)
		}
		ingestService, err = ingest.NewService(ingestConfig, collectorAuthorizer{
			store: tokenStore, tenantID: config.tenantID,
		}, eventStore)
		if err != nil {
			return fmt.Errorf("create collector ingestion service: %w", err)
		}
	}
	collectorServer, collectorListener, err := openCollectorServer(
		startupContext,
		collectorTransportConfig,
		ingestService,
	)
	if err != nil {
		return err
	}
	if collectorListener != nil {
		defer func() {
			if err := collectorListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Printf("close collector listener: %v", err)
			}
		}()
	}

	indexStatistics, err := newRuntimeIndexStatisticsReader(
		connection,
		internalclickhouse.IndexStatisticsConfig{
			Database: "open_splunk",
			Table:    "events",
		},
		indexReads.admission,
	)
	if err != nil {
		return fmt.Errorf("create index statistics reader: %w", err)
	}
	executor, err := newRuntimeQueryExecutor(
		connection,
		queryexec.Config{},
		indexReads.admission,
	)
	if err != nil {
		return fmt.Errorf("create query executor: %w", err)
	}
	compiler := internalclickhouse.Compiler{
		Database: "open_splunk",
		Table:    "events",
	}
	jobJournal, err := searchhistory.NewJobJournal(searchHistory)
	if err != nil {
		return fmt.Errorf("create search-history job journal: %w", err)
	}
	// Construct the catalog and its immutable resolver before search admission.
	// Manager captures this exact resolver in its private configuration; no
	// later HTTP composition can rotate search-time knowledge authority.
	knowledgeManagement, err := newRuntimeKnowledgeManagement(
		startupContext,
		controlDB,
		config.masterKeyPath,
		securityStores.auditEvents,
	)
	if err != nil {
		return err
	}
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor:          executor,
		Snapshotter:       visibilitySnapshotter{sequencer: sequencer},
		Journal:           jobJournal,
		KnowledgeResolver: knowledgeManagement.resolver,
		LookupResolver:    knowledgeManagement.lookupResolver,
		OnJournalError: func(err error) {
			log.Printf("persist search-job history: %v", err)
		},
		OnExecutionError: func(jobID string, code searchjobs.FailureCode, cause error) {
			log.Print(formatSearchExecutionFailure(jobID, code, cause))
		},
		Compiler: compiler,
	})
	if err != nil {
		return fmt.Errorf("create search job manager: %w", err)
	}
	defer func() {
		if err := jobs.Close(); err != nil {
			log.Printf("close search jobs: %v", err)
		}
	}()
	knowledgePreview, err := knowledgepreview.NewService(knowledgepreview.Config{
		Searches: jobs,
		Writer:   knowledgeManagement.writer,
		Compiler: knowledgepreview.ProductionCompilerAdapter{Compiler: compiler},
		Executor: executor,
	})
	if err != nil {
		return fmt.Errorf("create knowledge preview service: %w", err)
	}
	inspection, err := newRuntimeSearchInspection(
		runtimeSearchInspectionConfig{
			Searches:          jobs,
			Compiler:          compiler,
			ClickHouseOptions: clickHouseOptions.runtime,
		},
	)
	if err != nil {
		return fmt.Errorf("create search inspection services: %w", err)
	}
	// Registered after jobs so LIFO shutdown first stops inspection admission,
	// waits for active requests, and closes its isolated EXPLAIN lanes. The
	// borrowed search manager and shared ClickHouse connection remain alive
	// until afterward.
	defer func() {
		if err := inspection.Close(); err != nil {
			log.Printf("close search inspection services: %v", err)
		}
	}()
	exportExecutorConfig := exportSettings.queryExecutorConfig()
	exportExecutor, err := newRuntimeQueryExecutor(
		connection,
		exportExecutorConfig,
		indexReads.admission,
	)
	if err != nil {
		return fmt.Errorf("create export query executor: %w", err)
	}
	exportSource, err := exportjobs.NewReexecutionSource(exportjobs.ReexecutionSourceConfig{
		Searches:   jobs,
		Executor:   exportExecutor,
		Compiler:   compiler,
		MaxRuntime: exportSettings.reexecutionMaxRuntime,
	})
	if err != nil {
		return fmt.Errorf("create export re-execution source: %w", err)
	}
	exports, err := exportjobs.New(exportSettings.managerConfig(
		exportSource,
		config.exportArtifactDir,
		reportExportCleanupError,
	))
	if err != nil {
		return fmt.Errorf("create export manager: %w", err)
	}
	// Registered after jobs so LIFO shutdown always cancels export workers and
	// releases their search leases before the search-job manager is closed.
	defer func() {
		if err := exports.Close(); err != nil {
			log.Printf("close exports: %v", err)
		}
	}()
	analysis, err := newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches:         jobs,
		Compiler:         compiler,
		Executor:         executor,
		FieldCursorScope: "open-splunk-server/search-fields",
	})
	if err != nil {
		return fmt.Errorf("create search analysis services: %w", err)
	}
	// Registered after exports and jobs so LIFO shutdown cancels and joins all
	// suggestion operations and detached field-catalog workers before either
	// borrowed dependency closes. The later WebSocket safety close still runs
	// first.
	defer func() {
		if err := analysis.Close(); err != nil {
			log.Printf("close search analysis services: %v", err)
		}
	}()

	searchWebSocket, err := searchws.New(searchws.Config{
		Searches: jobs,
		Exports:  exports,
		Access: searchjobs.AccessScope{
			TenantID: config.tenantID,
			OwnerID:  defaultOwnerID,
		},
	})
	if err != nil {
		return fmt.Errorf("create search websocket service: %w", err)
	}
	// This safety close executes before export/search manager defers. Normal
	// runtime shutdown closes the same service through server.Handler first.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := searchWebSocket.Close(ctx); err != nil {
			log.Printf("close search websocket service: %v", err)
		}
	}()
	appCatalog, appCursorKey, err := newRuntimeAuditedAppCatalog(
		startupContext,
		controlDB,
		config.masterKeyPath,
		securityStores.auditEvents,
	)
	if err != nil {
		return err
	}
	collectorAdministration, err := newRuntimeCollectorAdministration(
		startupContext,
		controlDB,
		collectorFleet,
		collectorHeartbeats,
		config.masterKeyPath,
	)
	if err != nil {
		clear(appCursorKey)
		return err
	}
	httpConfig := server.Config{
		SearchJobs:                 jobs,
		RuntimeReadiness:           connection,
		SearchInspections:          inspection.service,
		SearchWebSocket:            searchWebSocket,
		Exports:                    exports,
		Indexes:                    controlDB,
		IndexAdmin:                 indexAdministration,
		IndexStatistics:            indexStatistics,
		IndexStatisticsSnapshotter: visibilitySnapshotter{sequencer: sequencer},
		IndexDataDeletionAdmission: indexAdministration,
		IndexDataDeletionWaker:     indexDataDeletion,
		IngestionTokens:            tokenStore,
		AuditEvents:                securityStores.auditEvents,
		SearchAttemptAuditEvents:   securityStores.searchAttemptAuditEvents,
		CollectorAdmin:             collectorAdministration,
		AppAdmin:                   appCatalog,
		AppCatalog:                 appCatalog,
		AppCursorKey:               appCursorKey,
		SavedSearches:              savedSearches,
		Dashboards:                 dashboardStore,
		SearchHistory:              searchHistory,
		WebUI:                      release.WebUI,
		OwnerID:                    defaultOwnerID,
		TenantID:                   config.tenantID,
		AdministrativeAllowedHosts: config.httpAllowedHosts,
		BrowserAuthenticator:       browserAuthenticator,
		Bootstrap: server.BootstrapConfig{
			Build:              buildMetadata,
			MaximumExportRows:  exportSettings.maximumRowLimit,
			MaximumExportBytes: exportSettings.maximumByteLimit,
		},
	}
	var hecMetrics *hechttp.Metrics
	if config.hecEnabled {
		hecMetrics = hechttp.NewMetrics()
		hecOperations, operationsErr := newRuntimeHECOperations(
			hecMetrics,
			sequencer,
			eventStore,
		)
		if operationsErr != nil {
			clear(appCursorKey)
			return fmt.Errorf("create HEC operational telemetry: %w", operationsErr)
		}
		httpConfig.HECOperations = hecOperations
		httpConfig.Bootstrap.Features = append(
			httpConfig.Bootstrap.Features,
			opensplunk.ServerFeature_SERVER_FEATURE_HEC_INGESTION,
		)
	}
	if err := configureRuntimeKnowledgeManagement(
		&httpConfig,
		knowledgeManagement,
		appCatalog,
		knowledgePreview,
	); err != nil {
		clear(appCursorKey)
		return err
	}
	browserHandler, err := newRuntimeHTTPHandler(httpConfig, analysis)
	// NewHandler clones the key before returning. Erase this temporary caller
	// copy immediately on both success and failure.
	clear(appCursorKey)
	if err != nil {
		return fmt.Errorf("create HTTP handler: %w", err)
	}
	var routedHandler http.Handler = browserHandler
	var admissionLifecycles []httpAdmissionShutdown
	if config.hecEnabled {
		hecHandler, hecErr := newRuntimeHECHandler(runtimeHECConfig{
			Next:                  browserHandler,
			Authenticator:         tokenStore,
			Store:                 eventStore,
			Sequencer:             sequencer,
			TenantID:              config.tenantID,
			DefaultIndexRetention: config.indexRetention,
			Metrics:               hecMetrics,
		})
		if hecErr != nil {
			return fmt.Errorf("create HEC HTTP handler: %w", hecErr)
		}
		routedHandler = hecHandler
		admissionLifecycles = append(admissionLifecycles, hecHandler)
	}
	// Track the complete selected HTTP surface, including HEC authentication
	// and framing work before token-scoped lifecycle admission.
	requests := newTrackedHandler(routedHandler)
	var rootHandler http.Handler = requests

	shutdownContext, rawStopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var stopSignalsOnce sync.Once
	stopSignals := func() { stopSignalsOnce.Do(rawStopSignals) }
	defer stopSignals()
	// Once graceful shutdown starts, restore the process's default signal
	// behavior. A second SIGINT/SIGTERM can then terminate a handler or driver
	// that ignores cancellation instead of being captured indefinitely.
	go func() {
		<-shutdownContext.Done()
		stopSignals()
	}()
	httpServer := &httpRuntimeServer{
		Server: &http.Server{
			Addr:              config.httpAddress,
			Handler:           rootHandler,
			TLSConfig:         httpTLSConfig,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       30 * time.Second,
			// Keep ordinary API writes short. The raw export handler explicitly
			// extends only its own connection deadline through ResponseController.
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    2 * time.Minute,
			MaxHeaderBytes: 1 << 20,
		},
	}
	httpTransport := "plaintext"
	if httpTLSConfig != nil {
		httpTransport = "TLS"
	}
	log.Printf(
		"open-splunk server listening on %s (%s)",
		config.httpAddress,
		httpTransport,
	)
	if config.hecEnabled {
		log.Printf("HEC enabled on the existing %s listener", httpTransport)
	}
	if collectorListener == nil {
		log.Printf("collector gRPC listener disabled; configure -collector-grpc-address and TLS to enable ingestion")
	} else {
		transport := "TLS"
		if config.collectorInsecure {
			transport = "explicit loopback plaintext"
		}
		log.Printf("collector gRPC server listening on %s (%s)", collectorListener.Addr(), transport)
	}
	serveErr := serveRuntime(
		shutdownContext,
		httpServer,
		requests,
		browserHandler,
		optionalRuntimeGRPCServer(collectorServer),
		collectorListener,
		shutdownTimeout,
		collectorShutdownGraceTimeout,
		admissionLifecycles...,
	)
	heartbeatCloseErr := closeCollectorHeartbeatRuntime(
		collectorHeartbeats,
		shutdownTimeout,
	)
	collectorHeartbeats = nil
	if heartbeatCloseErr != nil {
		heartbeatCloseErr = fmt.Errorf(
			"close collector heartbeat runtime: %w",
			heartbeatCloseErr,
		)
	}
	return errors.Join(serveErr, heartbeatCloseErr)
}

func writeEmbeddedReleaseVerification(
	output io.Writer,
	release opensplunkroot.Release,
) error {
	if output == nil {
		return errors.New("write embedded release verification: output is nil")
	}
	if err := buildinfo.WriteIdentity(output, buildinfo.Identity{
		SourceRevision: release.Metadata.SourceRevision,
		ProductVersion: release.Metadata.ProductVersion,
	}); err != nil {
		return err
	}
	_, err := fmt.Fprintf(
		output,
		"ui_build_id=%s\nui_sha256=%s\n",
		release.Metadata.UIBuildID,
		release.Metadata.UI.SHA256,
	)
	return err
}

func closeCollectorHeartbeatRuntime(
	runtime *collectorfleet.HeartbeatRuntime,
	timeout time.Duration,
) error {
	if runtime == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return runtime.Close(ctx)
}

func registerSearchAttemptAuditMaximumRetainedFlag(
	flags *flag.FlagSet,
	config *options,
) {
	flags.IntVar(
		&config.searchAttemptAuditMaximumRetainedAttempts,
		"search-attempt-audit-maximum-retained-attempts",
		int(searchaudit.DefaultMaximumRetainedAttempts),
		"maximum search-attempt audit events retained per tenant",
	)
}

func parseFlags() options {
	var result options
	flag.BoolVar(&result.verifyEmbeddedRelease, "verify-embedded-release", false, "verify the embedded release payload and exit before opening runtime resources")
	flag.StringVar(&result.httpAddress, "http-address", "127.0.0.1:8080", "browser/API listen address")
	flag.StringVar(&result.httpAllowedHostsCSV, "http-allowed-hosts", "", "comma-separated Host names allowed to use the browser API (defaults to the specific listen host)")
	flag.StringVar(
		&result.httpTLSCert,
		"http-tls-cert",
		"",
		"PEM certificate chain for HTTPS browser/API traffic (requires -http-tls-key)",
	)
	flag.StringVar(
		&result.httpTLSKey,
		"http-tls-key",
		"",
		"PEM private key for HTTPS browser/API traffic (requires -http-tls-cert)",
	)
	flag.StringVar(&result.controlDBPath, "control-db", "open-splunk.db", "SQLite control-plane path")
	flag.StringVar(&result.masterKeyPath, "master-key", "", "server master-key path (default: <control-db>.key)")
	flag.StringVar(
		&result.administratorTokenFile,
		"administrator-token-file",
		"",
		"owner-only administrator bearer-token file (mutually exclusive with OPEN_SPLUNK_ADMINISTRATOR_TOKEN; required mode 0400 or 0600)",
	)
	flag.StringVar(&result.exportArtifactDir, "export-artifact-dir", "", "private export-artifact base directory (default: <control-db>.exports)")
	flag.StringVar(&result.clickhouseAddress, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native-protocol address")
	flag.StringVar(&result.clickhouseDatabase, "clickhouse-database", "open_splunk", "ClickHouse database")
	flag.StringVar(
		&result.clickhouseUsername,
		"clickhouse-username",
		"default",
		"ClickHouse username for migrations and application operations",
	)
	registerClickHouseSkipMigrationsFlag(flag.CommandLine, &result)
	flag.StringVar(
		&result.clickhousePasswordFile,
		"clickhouse-password-file",
		"",
		"file containing the ClickHouse password (mutually exclusive with OPEN_SPLUNK_CLICKHOUSE_PASSWORD)",
	)
	flag.BoolVar(&result.clickhouseSecure, "clickhouse-secure", false, "use TLS for ClickHouse")
	flag.StringVar(
		&result.clickhouseCACertFile,
		"clickhouse-ca-cert",
		"",
		"PEM trust bundle for verified ClickHouse TLS (requires -clickhouse-secure)",
	)
	flag.StringVar(
		&result.clickhouseServerName,
		"clickhouse-server-name",
		"",
		"explicit DNS name or IP SAN to verify for ClickHouse TLS (requires -clickhouse-secure)",
	)
	flag.StringVar(&result.collectorAddress, "collector-grpc-address", "", "collector gRPC listen address (disabled when empty)")
	flag.BoolVar(&result.collectorInsecure, "collector-grpc-insecure", false, "explicitly allow plaintext collector gRPC on loopback only")
	flag.StringVar(&result.collectorTLSCert, "collector-tls-cert", "", "PEM certificate for collector gRPC TLS")
	flag.StringVar(&result.collectorTLSKey, "collector-tls-key", "", "PEM private key for collector gRPC TLS")
	registerHECEnabledFlag(flag.CommandLine, &result)
	flag.DurationVar(&result.indexRetention, "default-index-retention", defaultIndexRetention, "retention used when an index does not override it")
	flag.DurationVar(
		&result.searchHistoryMaximumAge,
		"search-history-maximum-age",
		searchhistory.DefaultMaximumAge,
		"maximum age of terminal search-history entries",
	)
	flag.IntVar(
		&result.searchHistoryMaximumEntriesPerOwner,
		"search-history-maximum-entries-per-owner",
		searchhistory.DefaultMaximumEntriesPerOwner,
		"maximum terminal entries retained per owner (pending attempts are capped separately at the same value)",
	)
	registerSearchAttemptAuditMaximumRetainedFlag(flag.CommandLine, &result)
	flag.StringVar(&result.tenantID, "tenant-id", "default", "single-node tenant identifier")
	flag.Parse()
	if strings.TrimSpace(result.masterKeyPath) == "" {
		result.masterKeyPath = result.controlDBPath + ".key"
	}
	return result
}

func openSecurityStores(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
) (*auth.Store, error) {
	stores, err := openSecurityStoreSet(
		ctx,
		db,
		masterKeyPath,
		"default",
		false,
		0,
	)
	if err != nil {
		return nil, err
	}
	return stores.ingestionTokens, nil
}

type securityStoreSet struct {
	savedSearches            *savedobjects.AuditedStore
	ingestionTokens          *auth.Store
	auditEvents              *audit.Store
	searchAttemptAuditEvents *searchaudit.Store
}

func openRuntimeSecurityStores(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
	tenantID string,
) (securityStoreSet, error) {
	return openRuntimeSecurityStoresWithSearchAttemptMaximum(
		ctx,
		db,
		masterKeyPath,
		tenantID,
		0,
	)
}

func openRuntimeSecurityStoresWithSearchAttemptMaximum(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
	tenantID string,
	maximumRetainedAttempts uint32,
) (securityStoreSet, error) {
	return openSecurityStoreSet(
		ctx,
		db,
		masterKeyPath,
		tenantID,
		true,
		maximumRetainedAttempts,
	)
}

func openSecurityStoreSet(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
	tenantID string,
	requireExplicitAuditActor bool,
	searchAttemptMaximumRetained uint32,
) (securityStoreSet, error) {
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return securityStoreSet{}, fmt.Errorf("open server master key: %w", err)
	}
	defer clear(masterKey)
	cursorKey, err := deriveServerKey(masterKey, "saved-search-cursors")
	if err != nil {
		return securityStoreSet{}, err
	}
	defer clear(cursorKey)
	digestKey, err := deriveServerKey(masterKey, "collector-token-digests")
	if err != nil {
		return securityStoreSet{}, err
	}
	defer clear(digestKey)
	auditCursorKey, err := deriveServerKey(masterKey, auditCursorKeyPurpose)
	if err != nil {
		return securityStoreSet{}, err
	}
	defer clear(auditCursorKey)
	searchAttemptAuditCursorKey, err := deriveServerKey(
		masterKey,
		searchAttemptAuditCursorKeyPurpose,
	)
	if err != nil {
		return securityStoreSet{}, err
	}
	defer clear(searchAttemptAuditCursorKey)

	rawSavedSearches, err := savedobjects.New(
		db,
		savedobjects.Options{CursorKey: cursorKey},
	)
	if err != nil {
		return securityStoreSet{}, fmt.Errorf("create saved-search store: %w", err)
	}
	auditEvents, err := audit.NewStoreWithContext(
		ctx,
		db,
		audit.StoreOptions{CursorKey: auditCursorKey},
	)
	if err != nil {
		return securityStoreSet{}, fmt.Errorf("create audit-event store: %w", err)
	}
	searchAttemptAuditEvents, err := searchaudit.NewWithContext(
		ctx,
		db,
		searchaudit.Options{
			CursorKey:               searchAttemptAuditCursorKey,
			MaximumRetainedAttempts: searchAttemptMaximumRetained,
		},
	)
	if err != nil {
		return securityStoreSet{}, fmt.Errorf(
			"create search-attempt audit store: %w",
			err,
		)
	}
	savedSearches, err := savedobjects.NewAuditedStore(
		rawSavedSearches,
		tenantID,
		auditEvents,
	)
	if err != nil {
		return securityStoreSet{}, fmt.Errorf(
			"create audited saved-search store: %w",
			err,
		)
	}
	tokens, err := auth.NewStoreWithOptions(db, digestKey, auth.StoreOptions{
		AuditAppender:             auditEvents,
		AuditTenantID:             tenantID,
		RequireExplicitAuditActor: requireExplicitAuditActor,
	})
	if err != nil {
		return securityStoreSet{}, fmt.Errorf("create collector-token store: %w", err)
	}
	return securityStoreSet{
		savedSearches:            savedSearches,
		ingestionTokens:          tokens,
		auditEvents:              auditEvents,
		searchAttemptAuditEvents: searchAttemptAuditEvents,
	}, nil
}

func openSearchHistoryStore(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
	options searchhistory.Options,
) (*searchhistory.Store, error) {
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("open search-history master key: %w", err)
	}
	defer clear(masterKey)
	cursorKey, err := deriveServerKey(masterKey, "search-history-cursors")
	if err != nil {
		return nil, err
	}
	defer clear(cursorKey)
	options.CursorKey = cursorKey
	store, err := searchhistory.New(db, options)
	if err != nil {
		return nil, fmt.Errorf("create search-history store: %w", err)
	}
	return store, nil
}

type clickHouseConnectionOptions struct {
	migration *clickhousedriver.Options
	runtime   *clickhousedriver.Options
	deletion  *clickhousedriver.Options
}

// #nosec G101 -- This is the identifier of an environment variable, not a credential.
const clickHousePasswordEnvironmentVariable = "OPEN_SPLUNK_CLICKHOUSE_PASSWORD"

func registerClickHouseSkipMigrationsFlag(
	flags *flag.FlagSet,
	config *options,
) {
	flags.BoolVar(
		&config.clickhouseSkipMigrations,
		"clickhouse-skip-migrations",
		false,
		"skip embedded ClickHouse migrations for a pre-provisioned schema",
	)
}

func registerHECEnabledFlag(
	flags *flag.FlagSet,
	config *options,
) {
	flags.BoolVar(
		&config.hecEnabled,
		"hec-enabled",
		false,
		"enable the complete HEC route set on the browser/API listener",
	)
}

func discardClickHouseMigrationCredentials(
	options *clickHouseConnectionOptions,
) {
	if options == nil || options.migration == nil {
		return
	}
	options.migration.Auth.Password = ""
	options.migration = nil
}

func discardClickHouseConnectionCredentials(
	options *clickHouseConnectionOptions,
) {
	if options == nil {
		return
	}
	for _, connectionOptions := range []*clickhousedriver.Options{
		options.migration,
		options.runtime,
		options.deletion,
	} {
		if connectionOptions != nil {
			connectionOptions.Auth.Password = ""
		}
	}
	*options = clickHouseConnectionOptions{}
}

func configureClickHouseConnectionOptions(
	config options,
	tlsProfile *clickHouseClientTLSProfile,
) (clickHouseConnectionOptions, error) {
	result, configureErr := newClickHouseConnectionOptions(config, tlsProfile)
	unsetErr := os.Unsetenv(clickHousePasswordEnvironmentVariable)
	if unsetErr != nil {
		unsetErr = fmt.Errorf(
			"discard ClickHouse password from process environment: %w",
			unsetErr,
		)
	}
	if configureErr == nil && unsetErr == nil {
		return result, nil
	}
	discardClickHouseConnectionCredentials(&result)
	return clickHouseConnectionOptions{}, errors.Join(configureErr, unsetErr)
}

func newClickHouseConnectionOptions(
	config options,
	tlsProfile *clickHouseClientTLSProfile,
) (clickHouseConnectionOptions, error) {
	if err := validateClickHouseClientTLSProfile(
		config.clickhouseSecure,
		tlsProfile,
	); err != nil {
		return clickHouseConnectionOptions{}, err
	}
	address := strings.TrimSpace(config.clickhouseAddress)
	if address == "" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: address is required",
		)
	}
	database := strings.TrimSpace(config.clickhouseDatabase)
	if database != "open_splunk" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: database must be open_splunk for " +
				"the embedded schema",
		)
	}
	username := strings.TrimSpace(config.clickhouseUsername)
	if username == "" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: username is required",
		)
	}
	password, err := loadClickHouseCredential(
		config.clickhousePasswordFile,
		clickHousePasswordEnvironmentVariable,
	)
	if err != nil {
		return clickHouseConnectionOptions{}, err
	}
	base := clickhousedriver.Options{
		Protocol:         clickhousedriver.Native,
		Addr:             []string{address},
		DialTimeout:      5 * time.Second,
		ReadTimeout:      30 * time.Second,
		MaxOpenConns:     8,
		MaxIdleConns:     4,
		ConnMaxLifetime:  30 * time.Minute,
		ConnOpenStrategy: clickhousedriver.ConnOpenInOrder,
	}
	runtimeOptions := base
	runtimeOptions.TLS = tlsProfile.newConfig()
	runtimeOptions.Auth = clickhousedriver.Auth{
		Database: database,
		Username: username,
		Password: password,
	}
	deletionOptions := base
	deletionOptions.TLS = tlsProfile.newConfig()
	deletionOptions.Auth = clickhousedriver.Auth{
		Database: database,
		Username: username,
		Password: password,
	}
	// The deletion coordinator is serialized and the startup contract uses
	// bounded sequential probes, so one privileged session is sufficient.
	deletionOptions.MaxOpenConns = 1
	deletionOptions.MaxIdleConns = 1
	result := clickHouseConnectionOptions{
		runtime:  &runtimeOptions,
		deletion: &deletionOptions,
	}
	if config.clickhouseSkipMigrations {
		return result, nil
	}
	migrationOptions := base
	migrationOptions.TLS = tlsProfile.newConfig()
	migrationOptions.Auth = clickhousedriver.Auth{
		// The first migration owns creation of the embedded database.
		Database: "default",
		Username: username,
		Password: password,
	}
	migrationOptions.MaxOpenConns = 1
	migrationOptions.MaxIdleConns = 1
	result.migration = &migrationOptions
	return result, nil
}

func openClickHouse(
	options *clickhousedriver.Options,
) (clickhousedriver.Conn, error) {
	if options == nil {
		return nil, errors.New(
			"open ClickHouse: options are required",
		)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}
	return connection, nil
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
