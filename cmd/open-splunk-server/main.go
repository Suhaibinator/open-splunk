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
	"github.com/Suhaibinator/open-splunk/internal/auth"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
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
	httpAddress                string
	httpAllowedHosts           []string
	httpAllowedHostsCSV        string
	httpInsecureTrustedNetwork bool
	controlDBPath              string
	masterKeyPath              string
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

	sequencer, err := visibility.NewSQLite(startupContext, controlDB)
	if err != nil {
		return fmt.Errorf("open visibility sequencer: %w", err)
	}
	savedSearches, tokenStore, err := openSecurityStores(startupContext, controlDB, config.masterKeyPath)
	if err != nil {
		return err
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
	connection, err := openClickHouse(config)
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
	ingestConfig.ServerVersion = "dev"
	ingestService, err := ingest.NewService(ingestConfig, collectorAuthorizer{
		store: tokenStore, tenantID: config.tenantID,
	}, eventStore)
	if err != nil {
		return fmt.Errorf("create collector ingestion service: %w", err)
	}
	collectorServer, collectorListener, err := openCollectorServer(collectorServerConfig{
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

	webUI, err := opensplunk.WebUI()
	if err != nil {
		return fmt.Errorf("open embedded web UI: %w", err)
	}
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
	handler, err := newRuntimeHTTPHandler(server.Config{
		SearchJobs:                 jobs,
		SearchWebSocket:            searchWebSocket,
		Exports:                    exports,
		Indexes:                    controlDB,
		IngestionTokens:            tokenStore,
		SavedSearches:              savedSearches,
		SearchHistory:              searchHistory,
		WebUI:                      webUI,
		OwnerID:                    defaultOwnerID,
		TenantID:                   config.tenantID,
		AdministrativeAllowedHosts: config.httpAllowedHosts,
		Bootstrap: server.BootstrapConfig{
			ServerVersion:           "dev",
			APIVersion:              "v1",
			SPLCompatibilityVersion: splCompatibility,
			MaximumExportRows:       exportSettings.maximumRowLimit,
			MaximumExportBytes:      exportSettings.maximumByteLimit,
		},
	}, searchanalysis.Config{
		Searches: jobs,
		Compiler: compiler,
		Executor: executor,
	})
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
	flag.StringVar(&result.httpAddress, "http-address", "127.0.0.1:8080", "HTTP listen address (set explicitly to expose on a trusted network)")
	flag.StringVar(&result.httpAllowedHostsCSV, "http-allowed-hosts", "", "comma-separated Host names allowed to use the browser API (defaults to the specific listen host)")
	flag.BoolVar(&result.httpInsecureTrustedNetwork, "http-insecure-trusted-network", false, "explicitly allow plaintext browser HTTP on a non-loopback trusted network")
	flag.StringVar(&result.controlDBPath, "control-db", "open-splunk.db", "SQLite control-plane path")
	flag.StringVar(&result.masterKeyPath, "master-key", "", "server master-key path (default: <control-db>.key)")
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

func openClickHouse(config options) (clickhousedriver.Conn, error) {
	address := strings.TrimSpace(config.clickhouseAddress)
	if address == "" {
		return nil, errors.New("open ClickHouse: address is required")
	}
	if !config.clickhouseSecure && !loopbackAddress(address) {
		return nil, errors.New("open ClickHouse: plaintext is allowed only for a loopback address; enable -clickhouse-secure")
	}
	database := strings.TrimSpace(config.clickhouseDatabase)
	if database != "open_splunk" {
		return nil, errors.New("open ClickHouse: database must be open_splunk for the embedded schema")
	}
	username := strings.TrimSpace(config.clickhouseUsername)
	if username == "" {
		return nil, errors.New("open ClickHouse: username is required")
	}
	options := &clickhousedriver.Options{
		Addr: []string{address},
		Auth: clickhousedriver.Auth{
			// Connect through ClickHouse's always-present bootstrap database so
			// the first migration can create open_splunk on a clean server. All
			// runtime SQL uses the fully qualified open_splunk schema.
			Database: "default",
			Username: username,
			Password: os.Getenv("OPEN_SPLUNK_CLICKHOUSE_PASSWORD"),
		},
		DialTimeout:     5 * time.Second,
		ReadTimeout:     30 * time.Second,
		MaxOpenConns:    8,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
	}
	if config.clickhouseSecure {
		options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
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
