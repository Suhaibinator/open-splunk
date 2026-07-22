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
	"syscall"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	startupTimeout  = 2 * time.Minute
	shutdownTimeout = 35 * time.Second
)

type options struct {
	httpAddress        string
	controlDBPath      string
	clickhouseAddress  string
	clickhouseDatabase string
	clickhouseUsername string
	clickhouseSecure   bool
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
	connection, err := openClickHouse(config)
	if err != nil {
		return err
	}
	defer func() {
		if err := connection.Close(); err != nil {
			log.Printf("close ClickHouse: %v", err)
		}
	}()
	if err := connection.Ping(startupContext); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}
	if err := server.ApplyClickHouseMigrations(startupContext, connection, migrations.ClickHouse()); err != nil {
		return fmt.Errorf("migrate ClickHouse: %w", err)
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
		SearchJobs: jobs,
		Indexes:    controlDB,
		WebUI:      webUI,
		TenantID:   config.tenantID,
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

	shutdownContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	httpServer := &http.Server{
		Addr:              config.httpAddress,
		Handler:           requests,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	shutdownDone := make(chan error, 1)
	go func() {
		<-shutdownContext.Done()
		shutdownDone <- shutdownHTTPServer(httpServer, requests, shutdownTimeout)
	}()

	log.Printf("open-splunk server listening on %s", config.httpAddress)
	serveErr := httpServer.ListenAndServe()
	stopSignals()
	shutdownErr := <-shutdownDone
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = fmt.Errorf("serve HTTP: %w", serveErr)
	} else {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr)
}

func parseFlags() options {
	var result options
	flag.StringVar(&result.httpAddress, "http-address", "127.0.0.1:8080", "HTTP listen address (set explicitly to expose on a trusted network)")
	flag.StringVar(&result.controlDBPath, "control-db", "open-splunk.db", "SQLite control-plane path")
	flag.StringVar(&result.clickhouseAddress, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native-protocol address")
	flag.StringVar(&result.clickhouseDatabase, "clickhouse-database", "open_splunk", "ClickHouse database")
	flag.StringVar(&result.clickhouseUsername, "clickhouse-username", "open_splunk", "ClickHouse username")
	flag.BoolVar(&result.clickhouseSecure, "clickhouse-secure", false, "use TLS for ClickHouse")
	flag.StringVar(&result.tenantID, "tenant-id", "default", "single-node tenant identifier")
	flag.Parse()
	return result
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
