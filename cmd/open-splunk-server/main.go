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
	startupTimeout        = 2 * time.Minute
	shutdownTimeout       = 35 * time.Second
	defaultIndexRetention = 30 * 24 * time.Hour
	defaultOwnerID        = "single-user"
	splCompatibility      = "tier-1-dev"
)

type options struct {
	verifyEmbeddedRelease      bool
	httpAddress                string
	httpAllowedHosts           []string
	httpAllowedHostsCSV        string
	httpInsecureTrustedNetwork bool
	controlDBPath              string
	masterKeyPath              string
	administratorTokenFile     string
	exportArtifactDir          string
	clickhouseAddress          string
	clickhouseDatabase         string
	clickhouseUsername         string
	clickhouseSecure           bool
	collectorAddress           string
	collectorInsecure          bool
	collectorTLSCert           string
	collectorTLSKey            string
	indexRetention             time.Duration
	tenantID                   string
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
	searchHistory, err := openSearchHistoryStore(startupContext, controlDB, config.masterKeyPath)
	if err != nil {
		return err
	}
	recoveredSearches, err := searchHistory.RecoverInterrupted(startupContext, searchhistory.AccessScope{
		TenantID: config.tenantID,
		OwnerID:  defaultOwnerID,
	})
	if err != nil {
		return fmt.Errorf("recover interrupted search history: %w", err)
	}
	if recoveredSearches != 0 {
		log.Printf("recovered %d interrupted search attempts", recoveredSearches)
	}
	clickHouseOptions, err := newClickHouseOptions(config)
	if err != nil {
		return err
	}
	connection, err := openClickHouse(clickHouseOptions)
	if err != nil {
		return err
	}
	var eventStore *internalclickhouse.Store
	defer func() {
		// Once NewStore succeeds, it owns the shared native connection and its
		// later defer closes it after search jobs and transports have stopped.
		if eventStore == nil {
			if err := connection.Close(); err != nil {
				log.Printf("close ClickHouse after failed startup: %v", err)
			}
		}
	}()
	if err := connection.Ping(startupContext); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}
	if err := server.ApplyClickHouseMigrations(startupContext, connection, migrations.ClickHouse()); err != nil {
		return fmt.Errorf("migrate ClickHouse: %w", err)
	}

	eventStore, err = internalclickhouse.NewStore(connection, controlRetentionProvider{
		catalog: controlDB, tenantID: config.tenantID, defaultRetention: config.indexRetention,
	}, sequencer)
	if err != nil {
		return fmt.Errorf("create ClickHouse ingestion store: %w", err)
	}
	defer func() {
		if err := eventStore.Close(); err != nil {
			log.Printf("close ClickHouse: %v", err)
		}
	}()
	ingestConfig := ingest.DefaultConfig()
	ingestConfig.Build = buildMetadata
	ingestConfig.ServerInstanceID = collectorBootEpoch
	ingestConfig.SessionManager = collectorSessionManager{
		admission: collectorAdmissions,
		fleet:     collectorFleet,
	}
	ingestConfig.SessionErrorHandler = func(err error) {
		log.Printf("collector session cleanup: %v", err)
	}
	ingestService, err := ingest.NewService(ingestConfig, collectorAuthorizer{
		store: tokenStore, tenantID: config.tenantID,
	}, eventStore)
	if err != nil {
		return fmt.Errorf("create collector ingestion service: %w", err)
	}
	collectorServer, collectorListener, err := openCollectorServer(startupContext, collectorServerConfig{
		Address:     config.collectorAddress,
		Insecure:    config.collectorInsecure,
		TLSCertFile: config.collectorTLSCert,
		TLSKeyFile:  config.collectorTLSKey,
	}, ingestService)
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
			ClickHouseOptions: clickHouseOptions,
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
	exports, err := exportjobs.New(exportSettings.managerConfig(exportSource, config.exportArtifactDir))
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
	handler, err := newRuntimeHTTPHandler(server.Config{
		SearchJobs:                 jobs,
		SearchInspections:          inspection.service,
		SearchWebSocket:            searchWebSocket,
		Exports:                    exports,
		Indexes:                    controlDB,
		IngestionTokens:            tokenStore,
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
	return serveRuntime(shutdownContext, httpServer, requests, handler, collectorServer, collectorListener, shutdownTimeout)
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
	flag.StringVar(&result.clickhouseUsername, "clickhouse-username", "open_splunk", "ClickHouse username")
	flag.BoolVar(&result.clickhouseSecure, "clickhouse-secure", false, "use TLS for ClickHouse")
	flag.StringVar(&result.collectorAddress, "collector-grpc-address", "", "collector gRPC listen address (disabled when empty)")
	flag.BoolVar(&result.collectorInsecure, "collector-grpc-insecure", false, "explicitly allow plaintext collector gRPC on loopback only")
	flag.StringVar(&result.collectorTLSCert, "collector-tls-cert", "", "PEM certificate for collector gRPC TLS")
	flag.StringVar(&result.collectorTLSKey, "collector-tls-key", "", "PEM private key for collector gRPC TLS")
	flag.DurationVar(&result.indexRetention, "default-index-retention", defaultIndexRetention, "retention used when an index does not override it")
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

func openSearchHistoryStore(ctx context.Context, db *control.DB, masterKeyPath string) (*searchhistory.Store, error) {
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
	store, err := searchhistory.New(db, searchhistory.Options{CursorKey: cursorKey})
	if err != nil {
		return nil, fmt.Errorf("create search-history store: %w", err)
	}
	return store, nil
}

func newClickHouseOptions(
	config options,
) (*clickhousedriver.Options, error) {
	address := strings.TrimSpace(config.clickhouseAddress)
	if address == "" {
		return nil, errors.New(
			"configure ClickHouse: address is required",
		)
	}
	if !config.clickhouseSecure && !loopbackAddress(address) {
		return nil, errors.New(
			"configure ClickHouse: plaintext is allowed only for a " +
				"loopback address; enable -clickhouse-secure",
		)
	}
	database := strings.TrimSpace(config.clickhouseDatabase)
	if database != "open_splunk" {
		return nil, errors.New(
			"configure ClickHouse: database must be open_splunk for " +
				"the embedded schema",
		)
	}
	username := strings.TrimSpace(config.clickhouseUsername)
	if username == "" {
		return nil, errors.New(
			"configure ClickHouse: username is required",
		)
	}
	result := &clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{address},
		Auth: clickhousedriver.Auth{
			// Connect through ClickHouse's always-present bootstrap database so
			// the first migration can create open_splunk on a clean server. All
			// runtime SQL uses the fully qualified open_splunk schema.
			Database: "default",
			Username: username,
			Password: os.Getenv("OPEN_SPLUNK_CLICKHOUSE_PASSWORD"),
		},
		DialTimeout:      5 * time.Second,
		ReadTimeout:      30 * time.Second,
		MaxOpenConns:     8,
		MaxIdleConns:     4,
		ConnMaxLifetime:  30 * time.Minute,
		ConnOpenStrategy: clickhousedriver.ConnOpenInOrder,
	}
	if config.clickhouseSecure {
		result.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
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
