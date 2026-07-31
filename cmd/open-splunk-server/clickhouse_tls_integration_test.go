package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// TestClickHouseTLSServicePrincipalStartupLifecycle is opt-in because it starts
// a digest-pinned Docker container. It exercises the production TLS loader,
// principal-specific options, short-lived startup migration session, and both
// long-lived privilege validators against the real secure native protocol.
func TestClickHouseTLSServicePrincipalStartupLifecycle(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker integration was requested but the CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	container, err := testsupport.StartSecureClickHouseWithServicePrincipals(
		ctx,
		image,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close secure ClickHouse fixture: %v", closeErr)
		}
	})

	t.Setenv("OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD", container.MigrationPassword)
	t.Setenv("OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD", container.RuntimePassword)
	t.Setenv("OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD", container.DeletionPassword)
	config := secureClickHouseFixtureOptions(container)
	tlsProfile, err := loadClickHouseClientTLSProfile(
		true,
		container.TLSCACertificateFile,
		container.TLSServerName,
	)
	if err != nil {
		t.Fatalf("load production ClickHouse TLS config: %v", err)
	}
	connectionOptions, err := newClickHouseConnectionOptions(config, tlsProfile)
	if err != nil {
		t.Fatalf("build production ClickHouse connection options: %v", err)
	}
	for principal, principalOptions := range map[string]*clickhousedriver.Options{
		"migration": connectionOptions.migration,
		"runtime":   connectionOptions.runtime,
		"deletion":  connectionOptions.deletion,
	} {
		if principalOptions == nil || principalOptions.TLS == nil ||
			principalOptions.TLS.ServerName != container.TLSServerName ||
			principalOptions.TLS.RootCAs == nil {
			t.Fatalf("%s production TLS options = %#v", principal, principalOptions)
		}
	}

	migrationOptions := connectionOptions.migration
	if err := applyStartupClickHouseMigrations(
		ctx,
		migrationOptions,
		migrations.ClickHouse(),
		func(options *clickhousedriver.Options) (clickHouseMigrationSession, error) {
			return openClickHouse(options)
		},
		server.ApplyClickHouseMigrations,
	); err != nil {
		t.Fatalf("apply production startup migrations over TLS: %v", err)
	}
	discardClickHouseMigrationCredentials(&connectionOptions)
	if connectionOptions.migration != nil || migrationOptions.Auth.Password != "" {
		t.Fatal("startup retained migration credentials")
	}

	runtimeConnection := openLiveClickHouseConnection(
		t,
		connectionOptions.runtime,
		"runtime",
	)
	deletionConnection := openLiveClickHouseConnection(
		t,
		connectionOptions.deletion,
		"deletion",
	)
	t.Cleanup(func() {
		if closeErr := deletionConnection.Close(); closeErr != nil {
			t.Errorf("close TLS deletion connection: %v", closeErr)
		}
		if closeErr := runtimeConnection.Close(); closeErr != nil {
			t.Errorf("close TLS runtime connection: %v", closeErr)
		}
	})
	if err := runtimeConnection.Ping(ctx); err != nil {
		t.Fatalf("ping runtime principal over TLS: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(ctx, runtimeConnection); err != nil {
		t.Fatalf("validate runtime principal over TLS: %v", err)
	}
	if err := deletionConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deletion principal over TLS: %v", err)
	}
	if err := server.ValidateClickHouseDeletionWorkerPrivileges(ctx, deletionConnection); err != nil {
		t.Fatalf("validate deletion principal over TLS: %v", err)
	}
	var eventRows uint64
	if err := runtimeConnection.QueryRow(
		ctx,
		"SELECT count() FROM open_splunk.events",
	).Scan(&eventRows); err != nil {
		t.Fatalf("query migrated schema over TLS runtime session: %v", err)
	}
	if err := deletionConnection.Exec(
		ctx,
		"ALTER TABLE open_splunk.events DELETE WHERE tenant_id = ? AND index_name = ?",
		"tls-fixture-tenant",
		"tls-fixture-index",
	); err != nil {
		t.Fatalf("exercise deletion privilege over TLS: %v", err)
	}
	explainQuery := liveClickHouseTLSExplainQuery(t)
	explainer, err := queryexec.NewExplainer(
		connectionOptions.runtime,
		queryexec.Config{},
	)
	if err != nil {
		t.Fatalf("create production EXPLAIN TLS lanes: %v", err)
	}
	explained, err := explainer.Explain(ctx, explainQuery)
	if err != nil {
		_ = explainer.Close()
		t.Fatalf("execute production EXPLAIN over TLS: %v", err)
	}
	if strings.TrimSpace(explained.Text) == "" || explained.QueryID == "" {
		_ = explainer.Close()
		t.Fatalf("production TLS EXPLAIN result = %#v", explained)
	}
	if err := explainer.Close(); err != nil {
		t.Fatalf("close production EXPLAIN TLS lanes: %v", err)
	}

	t.Run("wrong hostname", func(t *testing.T) {
		wrongTLS, loadErr := loadClickHouseClientTLSProfile(
			true,
			container.TLSCACertificateFile,
			"wrong.clickhouse.test",
		)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		wrongOptions, optionsErr := newClickHouseConnectionOptions(config, wrongTLS)
		if optionsErr != nil {
			t.Fatal(optionsErr)
		}
		requireLiveClickHousePingFailure(
			t,
			ctx,
			wrongOptions.runtime,
			"certificate",
			"x509",
		)
		requireLiveClickHouseExplainFailure(
			t,
			ctx,
			wrongOptions.runtime,
			explainQuery,
			"query failed",
			"certificate",
			"x509",
		)
	})

	t.Run("wrong CA", func(t *testing.T) {
		untrusted, identityErr := testsupport.WriteServerTLSIdentity(
			t.TempDir(),
			container.TLSServerName,
		)
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		wrongTLS, loadErr := loadClickHouseClientTLSProfile(
			true,
			untrusted.CertificateFile,
			container.TLSServerName,
		)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		wrongOptions, optionsErr := newClickHouseConnectionOptions(config, wrongTLS)
		if optionsErr != nil {
			t.Fatal(optionsErr)
		}
		requireLiveClickHousePingFailure(
			t,
			ctx,
			wrongOptions.runtime,
			"certificate",
			"x509",
			"unknown authority",
		)
		requireLiveClickHouseExplainFailure(
			t,
			ctx,
			wrongOptions.runtime,
			explainQuery,
			"query failed",
			"certificate",
			"x509",
			"unknown authority",
		)
	})

	t.Run("plaintext", func(t *testing.T) {
		plaintextConfig := config
		plaintextConfig.clickhouseSecure = false
		plaintextOptions, optionsErr := newClickHouseConnectionOptions(
			plaintextConfig,
			nil,
		)
		if optionsErr != nil {
			t.Fatal(optionsErr)
		}
		requireLiveClickHousePingFailure(
			t,
			ctx,
			plaintextOptions.runtime,
			"eof",
			"handshake",
			"connection reset",
			"unexpected packet",
		)
	})
}

func liveClickHouseTLSExplainQuery(t *testing.T) internalclickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse("index=tls-fixture-index")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, time.July, 31, 12, 0, 0, 0, time.UTC)
	visibilityCutoff := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tls-fixture-tenant",
		AuthorizedIndexes: []string{"tls-fixture-index"},
		RequestedIndexes:  []string{"tls-fixture-index"},
		Earliest:          anchor.Add(-time.Hour),
		Latest:            anchor.Add(time.Hour),
		SearchStart:       anchor,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   anchor,
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (internalclickhouse.Compiler{
		Database: "open_splunk",
		Table:    "events",
	}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func secureClickHouseFixtureOptions(
	container *testsupport.ClickHouseContainer,
) options {
	return options{
		clickhouseAddress:           container.Address,
		clickhouseDatabase:          container.Database,
		clickhouseRuntimeUsername:   container.RuntimeUsername,
		clickhouseDeletionUsername:  container.DeletionUsername,
		clickhouseMigrationUsername: container.MigrationUsername,
		clickhouseSecure:            true,
	}
}

func openLiveClickHouseConnection(
	t *testing.T,
	options *clickhousedriver.Options,
	principal string,
) clickhousedriver.Conn {
	t.Helper()
	connection, err := openClickHouse(options)
	if err != nil {
		t.Fatalf("open TLS %s connection: %v", principal, err)
	}
	return connection
}

func requireLiveClickHousePingFailure(
	t *testing.T,
	ctx context.Context,
	options *clickhousedriver.Options,
	diagnosticFragments ...string,
) {
	t.Helper()
	connection, err := openClickHouse(options)
	if err == nil {
		defer func() { _ = connection.Close() }()
		err = connection.Ping(ctx)
	}
	if err == nil {
		t.Fatal("invalid ClickHouse transport unexpectedly connected")
	}
	requireDiagnosticContainsAny(
		t,
		"invalid ClickHouse transport",
		err,
		diagnosticFragments...,
	)
}

func requireLiveClickHouseExplainFailure(
	t *testing.T,
	ctx context.Context,
	options *clickhousedriver.Options,
	query internalclickhouse.CompiledQuery,
	diagnosticFragments ...string,
) {
	t.Helper()
	explainer, err := queryexec.NewExplainer(options, queryexec.Config{})
	if err == nil {
		_, err = explainer.Explain(ctx, query)
		closeErr := explainer.Close()
		if err == nil {
			err = closeErr
		}
	}
	if err == nil {
		t.Fatal("invalid ClickHouse EXPLAIN transport unexpectedly connected")
	}
	requireDiagnosticContainsAny(
		t,
		"invalid ClickHouse EXPLAIN transport",
		err,
		diagnosticFragments...,
	)
}

func requireDiagnosticContainsAny(
	t *testing.T,
	operation string,
	err error,
	diagnosticFragments ...string,
) {
	t.Helper()
	diagnostic := strings.ToLower(err.Error())
	for _, fragment := range diagnosticFragments {
		if strings.Contains(diagnostic, fragment) {
			return
		}
	}
	t.Fatalf(
		"%s error = %v, want one of %q",
		operation,
		err,
		diagnosticFragments,
	)
}
