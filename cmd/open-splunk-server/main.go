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
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	startupTimeout        = 2 * time.Minute
	shutdownTimeout       = 35 * time.Second
	defaultIndexRetention = 30 * 24 * time.Hour
)

type options struct {
	httpAddress        string
	controlDBPath      string
	masterKeyPath      string
	clickhouseAddress  string
	clickhouseDatabase string
	clickhouseUsername string
	clickhouseSecure   bool
	collectorAddress   string
	collectorInsecure  bool
	collectorTLSCert   string
	collectorTLSKey    string
	indexRetention     time.Duration
	tenantID           string
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
	if config.indexRetention <= 0 {
		return errors.New("default index retention must be positive")
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
	jobs, err := searchjobs.New(searchjobs.Config{
		Executor:    executor,
		Snapshotter: visibilitySnapshotter{sequencer: sequencer},
		Compiler: internalclickhouse.Compiler{
			Database: "open_splunk",
			Table:    "events",
		},
	})
	if err != nil {
		return fmt.Errorf("create search job manager: %w", err)
	}
	defer func() {
		if err := jobs.Close(); err != nil {
			log.Printf("close search jobs: %v", err)
		}
	}()

	webUI, err := opensplunk.WebUI()
	if err != nil {
		return fmt.Errorf("open embedded web UI: %w", err)
	}
	handler, err := server.NewHandler(server.Config{
		SearchJobs:    jobs,
		Indexes:       controlDB,
		SavedSearches: savedSearches,
		WebUI:         webUI,
		TenantID:      config.tenantID,
		Bootstrap: server.BootstrapConfig{
			ServerVersion:           "dev",
			APIVersion:              "v1",
			SPLCompatibilityVersion: "tier-1-dev",
		},
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
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
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
	return serveRuntime(shutdownContext, httpServer, requests, collectorServer, collectorListener, shutdownTimeout)
}

func parseFlags() options {
	var result options
	flag.StringVar(&result.httpAddress, "http-address", "127.0.0.1:8080", "HTTP listen address (set explicitly to expose on a trusted network)")
	flag.StringVar(&result.controlDBPath, "control-db", "open-splunk.db", "SQLite control-plane path")
	flag.StringVar(&result.masterKeyPath, "master-key", "", "server master-key path (default: <control-db>.key)")
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
