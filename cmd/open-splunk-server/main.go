package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	startupTimeout                   = 2 * time.Minute
	shutdownTimeout                  = 35 * time.Second
	defaultIndexRetention            = 30 * 24 * time.Hour
	defaultOwnerID                   = "single-user"
	splCompatibility                 = "tier-1-dev"
	collectorHeartbeatFlushInterval  = time.Second
	collectorHeartbeatWriteTimeout   = 5 * time.Second
	collectorHeartbeatReleaseTimeout = 2 * collectorHeartbeatWriteTimeout
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
	verifyEmbeddedRelease               bool
	httpAddress                         string
	httpAllowedHosts                    []string
	httpAllowedHostsCSV                 string
	httpInsecureTrustedNetwork          bool
	controlDBPath                       string
	masterKeyPath                       string
	administratorTokenFile              string
	exportArtifactDir                   string
	clickhouseAddress                   string
	clickhouseDatabase                  string
	clickhouseRuntimeUsername           string
	clickhouseDeletionUsername          string
	clickhouseMigrationUsername         string
	clickhouseSecure                    bool
	collectorAddress                    string
	collectorInsecure                   bool
	collectorTLSCert                    string
	collectorTLSKey                     string
	indexRetention                      time.Duration
	searchHistoryMaximumAge             time.Duration
	searchHistoryMaximumEntriesPerOwner int
	tenantID                            string
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
	config := parseFlags()
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
	release, err := opensplunk.EmbeddedRelease()
	if err != nil {
		return fmt.Errorf("open embedded release: %w", err)
	}
	if config.verifyEmbeddedRelease {
		if err := buildinfo.WriteIdentity(os.Stdout, buildinfo.Identity{
			ApplicationVersion: release.Metadata.ApplicationVersion,
			SourceRevision:     release.Metadata.SourceRevision,
		}); err != nil {
			return err
		}
		_, err := fmt.Fprintf(
			os.Stdout,
			"ui_build_id=%s\nui_sha256=%s\n",
			release.Metadata.UIBuildID,
			release.Metadata.UI.SHA256,
		)
		return err
	}
	browserAuthenticator, err := newAdministratorBrowserAuthenticator(
		config.administratorTokenFile,
		config.tenantID,
		defaultOwnerID,
	)
	if err != nil {
		return err
	}
	buildMetadata := &opensplunkv1.BuildMetadata{
		ApplicationVersion:         release.Metadata.ApplicationVersion,
		SourceRevision:             release.Metadata.SourceRevision,
		UiBuildId:                  release.Metadata.UIBuildID,
		UiSha256:                   release.Metadata.UI.SHA256,
		ProtobufSchemaSha256:       release.Metadata.ProtobufSchema.SHA256,
		SqliteMigrationsSha256:     release.Metadata.SQLiteMigrations.SHA256,
		SqliteMigrationVersion:     release.Metadata.SQLiteMigrations.LatestVersion,
		ClickhouseMigrationsSha256: release.Metadata.ClickHouseMigrations.SHA256,
		ClickhouseMigrationVersion: release.Metadata.ClickHouseMigrations.LatestVersion,
		AssetManifestFormatVersion: release.Metadata.FormatVersion,
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
	savedSearches, tokenStore, err := openSecurityStores(startupContext, controlDB, config.masterKeyPath)
	if err != nil {
		return err
	}
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
			MaximumAge:             config.searchHistoryMaximumAge,
			MaximumEntriesPerOwner: config.searchHistoryMaximumEntriesPerOwner,
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
	clickHouseOptions, err := newClickHouseConnectionOptions(config)
	if err != nil {
		return err
	}
	defer discardClickHouseMigrationCredentials(&clickHouseOptions)
	if err := os.Unsetenv(
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
	); err != nil {
		return fmt.Errorf(
			"discard ClickHouse migration password from process environment: %w",
			err,
		)
	}
	if err := applyStartupClickHouseMigrations(
		startupContext,
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
	if err := server.ValidateClickHouseRuntimePrivileges(
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
	if err := server.ValidateClickHouseDeletionWorkerPrivileges(
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
	indexDataDeletion, err = newIndexDataDeletionRuntime(
		controlDB,
		eventStore,
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
	var collectorHeartbeats *collectorfleet.HeartbeatRuntime
	var ingestService opensplunkv1.CollectorIngestServiceServer
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

	executor, err := queryexec.New(connection, queryexec.Config{})
	if err != nil {
		return fmt.Errorf("create query executor: %w", err)
	}
	compiler := internalclickhouse.Compiler{
		Database: "open_splunk",
		Table:    "events",
	}
	jobJournal, err := searchhistory.NewJobJournal(searchHistory, splCompatibility)
	if err != nil {
		return fmt.Errorf("create search-history job journal: %w", err)
	}
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor:    executor,
		Snapshotter: visibilitySnapshotter{sequencer: sequencer},
		Journal:     jobJournal,
		OnJournalError: func(err error) {
			log.Printf("persist search-job history: %v", err)
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
	exportExecutor, err := queryexec.New(connection, exportSettings.queryExecutorConfig())
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
	appCatalog, appCursorKey, err := newRuntimeAppCatalog(
		startupContext,
		controlDB,
		config.masterKeyPath,
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
	handler, err := newRuntimeHTTPHandler(server.Config{
		SearchJobs:                 jobs,
		SearchInspections:          inspection.service,
		SearchWebSocket:            searchWebSocket,
		Exports:                    exports,
		Indexes:                    controlDB,
		IndexDataDeletionAdmission: controlDB,
		IndexDataDeletionWaker:     indexDataDeletion,
		IngestionTokens:            tokenStore,
		CollectorAdmin:             collectorAdministration,
		AppAdmin:                   appCatalog,
		AppCatalog:                 appCatalog,
		AppCursorKey:               appCursorKey,
		SavedSearches:              savedSearches,
		SearchHistory:              searchHistory,
		WebUI:                      release.WebUI,
		OwnerID:                    defaultOwnerID,
		TenantID:                   config.tenantID,
		AdministrativeAllowedHosts: config.httpAllowedHosts,
		BrowserAuthenticator:       browserAuthenticator,
		Bootstrap: server.BootstrapConfig{
			APIVersion:              "v1",
			SPLCompatibilityVersion: splCompatibility,
			Build:                   buildMetadata,
			MaximumExportRows:       exportSettings.maximumRowLimit,
			MaximumExportBytes:      exportSettings.maximumByteLimit,
		},
	}, analysis)
	// NewHandler clones the key before returning. Erase this temporary caller
	// copy immediately on both success and failure.
	clear(appCursorKey)
	if err != nil {
		return fmt.Errorf("create HTTP handler: %w", err)
	}
	requests := newTrackedHandler(handler)

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
	httpServer := &http.Server{
		Addr:              config.httpAddress,
		Handler:           requests,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Keep ordinary API writes short. The raw export handler explicitly
		// extends only its own connection deadline through ResponseController.
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	log.Printf("open-splunk server listening on %s", config.httpAddress)
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
		handler,
		collectorServer,
		collectorListener,
		shutdownTimeout,
		collectorShutdownGraceTimeout,
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

func parseFlags() options {
	var result options
	flag.BoolVar(&result.verifyEmbeddedRelease, "verify-embedded-release", false, "verify the embedded release payload and exit before opening runtime resources")
	flag.StringVar(&result.httpAddress, "http-address", "127.0.0.1:8080", "HTTP listen address (administrator browser routes currently require loopback)")
	flag.StringVar(&result.httpAllowedHostsCSV, "http-allowed-hosts", "", "comma-separated Host names allowed to use the browser API (defaults to the specific listen host)")
	flag.BoolVar(
		&result.httpInsecureTrustedNetwork,
		"http-insecure-trusted-network",
		false,
		"deprecated compatibility flag; administrator browser routes remain loopback-only until HTTPS is configured",
	)
	flag.StringVar(&result.controlDBPath, "control-db", "open-splunk.db", "SQLite control-plane path")
	flag.StringVar(&result.masterKeyPath, "master-key", "", "server master-key path (default: <control-db>.key)")
	flag.StringVar(
		&result.administratorTokenFile,
		"administrator-token-file",
		"",
		"required owner-only administrator bearer-token file (provision first with a CSPRNG, for example: umask 077; openssl rand -base64 48 > FILE; required mode 0400 or 0600)",
	)
	flag.StringVar(&result.exportArtifactDir, "export-artifact-dir", "", "private export-artifact base directory (default: <control-db>.exports)")
	flag.StringVar(&result.clickhouseAddress, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native-protocol address")
	flag.StringVar(&result.clickhouseDatabase, "clickhouse-database", "open_splunk", "ClickHouse database")
	flag.StringVar(
		&result.clickhouseRuntimeUsername,
		"clickhouse-runtime-username",
		"open_splunk_runtime",
		"least-privilege ClickHouse username for ingestion, search, and inspection",
	)
	flag.StringVar(
		&result.clickhouseDeletionUsername,
		"clickhouse-deletion-username",
		"open_splunk_deletion",
		"ClickHouse username limited to physical index deletion",
	)
	flag.StringVar(
		&result.clickhouseMigrationUsername,
		"clickhouse-migration-username",
		"open_splunk_migrator",
		"short-lived ClickHouse schema-migration username",
	)
	flag.BoolVar(&result.clickhouseSecure, "clickhouse-secure", false, "use TLS for ClickHouse")
	flag.StringVar(&result.collectorAddress, "collector-grpc-address", "", "collector gRPC listen address (disabled when empty)")
	flag.BoolVar(&result.collectorInsecure, "collector-grpc-insecure", false, "explicitly allow plaintext collector gRPC on loopback only")
	flag.StringVar(&result.collectorTLSCert, "collector-tls-cert", "", "PEM certificate for collector gRPC TLS")
	flag.StringVar(&result.collectorTLSKey, "collector-tls-key", "", "PEM private key for collector gRPC TLS")
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
	flag.StringVar(&result.tenantID, "tenant-id", "default", "single-node tenant identifier")
	flag.Parse()
	if strings.TrimSpace(result.masterKeyPath) == "" {
		result.masterKeyPath = result.controlDBPath + ".key"
	}
	return result
}

func openSecurityStores(ctx context.Context, db *control.DB, masterKeyPath string) (*savedobjects.Store, *auth.Store, error) {
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open server master key: %w", err)
	}
	defer clear(masterKey)
	cursorKey, err := deriveServerKey(masterKey, "saved-search-cursors")
	if err != nil {
		return nil, nil, err
	}
	defer clear(cursorKey)
	digestKey, err := deriveServerKey(masterKey, "collector-token-digests")
	if err != nil {
		return nil, nil, err
	}
	defer clear(digestKey)

	savedSearches, err := savedobjects.New(db, savedobjects.Options{CursorKey: cursorKey})
	if err != nil {
		return nil, nil, fmt.Errorf("create saved-search store: %w", err)
	}
	tokens, err := auth.NewStore(db, digestKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create collector-token store: %w", err)
	}
	return savedSearches, tokens, nil
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

func discardClickHouseMigrationCredentials(
	options *clickHouseConnectionOptions,
) {
	if options == nil || options.migration == nil {
		return
	}
	options.migration.Auth.Password = ""
	options.migration = nil
}

func newClickHouseConnectionOptions(
	config options,
) (clickHouseConnectionOptions, error) {
	address := strings.TrimSpace(config.clickhouseAddress)
	if address == "" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: address is required",
		)
	}
	if !config.clickhouseSecure && !loopbackAddress(address) {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: plaintext is allowed only for a " +
				"loopback address; enable -clickhouse-secure",
		)
	}
	database := strings.TrimSpace(config.clickhouseDatabase)
	if database != "open_splunk" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: database must be open_splunk for " +
				"the embedded schema",
		)
	}
	runtimeUsername := strings.TrimSpace(config.clickhouseRuntimeUsername)
	deletionUsername := strings.TrimSpace(config.clickhouseDeletionUsername)
	migrationUsername := strings.TrimSpace(config.clickhouseMigrationUsername)
	if runtimeUsername == "" || deletionUsername == "" ||
		migrationUsername == "" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: runtime, deletion, and migration usernames are required",
		)
	}
	if runtimeUsername == deletionUsername ||
		runtimeUsername == migrationUsername ||
		deletionUsername == migrationUsername {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: runtime, deletion, and migration usernames must be distinct",
		)
	}
	runtimePassword := os.Getenv(
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
	)
	deletionPassword := os.Getenv(
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
	)
	migrationPassword := os.Getenv(
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
	)
	if runtimePassword == "" || deletionPassword == "" ||
		migrationPassword == "" {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD, OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD, and OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD are required",
		)
	}
	if runtimePassword == deletionPassword ||
		runtimePassword == migrationPassword ||
		deletionPassword == migrationPassword {
		return clickHouseConnectionOptions{}, errors.New(
			"configure ClickHouse: runtime, deletion, and migration passwords must be distinct",
		)
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
	if config.clickhouseSecure {
		base.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	runtimeOptions := base
	runtimeOptions.Auth = clickhousedriver.Auth{
		Database: database,
		Username: runtimeUsername,
		Password: runtimePassword,
	}
	deletionOptions := base
	deletionOptions.Auth = clickhousedriver.Auth{
		Database: database,
		Username: deletionUsername,
		Password: deletionPassword,
	}
	// The deletion coordinator is serialized and the startup contract uses
	// bounded sequential probes, so one privileged session is sufficient.
	deletionOptions.MaxOpenConns = 1
	deletionOptions.MaxIdleConns = 1
	migrationOptions := base
	migrationOptions.Auth = clickhousedriver.Auth{
		// The first migration owns creation of the embedded database.
		Database: "default",
		Username: migrationUsername,
		Password: migrationPassword,
	}
	migrationOptions.MaxOpenConns = 1
	migrationOptions.MaxIdleConns = 1
	return clickHouseConnectionOptions{
		migration: &migrationOptions,
		runtime:   &runtimeOptions,
		deletion:  &deletionOptions,
	}, nil
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
