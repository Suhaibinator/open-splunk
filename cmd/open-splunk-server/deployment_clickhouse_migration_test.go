package main

import (
	"context"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestDeploymentClickHouseMigrationUsesOneVerifiedPrivilegedSession(
	t *testing.T,
) {
	identity, err := testsupport.WriteServerTLSIdentity(
		t.TempDir(),
		"clickhouse",
	)
	if err != nil {
		t.Fatal(err)
	}
	const password = "one-shot-migration-secret"
	passwordFile := writeClickHouseCredentialFixture(t, password+"\n", 0o600)
	passwordEnvironments := []string{
		"CLICKHOUSE_PASSWORD",
		clickHouseMigrationEnvironmentVariable,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
	}
	for _, name := range passwordEnvironments {
		t.Setenv(name, "must-be-discarded")
	}

	connection := &startupMigrationConnection{}
	opened := false
	var openedOptions *clickhousedriver.Options
	applied := false
	validated := false
	err = runDeploymentClickHouseMigrationWithDependencies(
		deploymentClickHouseMigrationOptions{
			Address:      "clickhouse:9440",
			PasswordFile: passwordFile,
			CACertFile:   identity.CertificateFile,
			ServerName:   "clickhouse",
		},
		deploymentClickHouseMigrationDependencies{
			migrationFiles: migrations.ClickHouse(),
			open: func(
				options *clickhousedriver.Options,
			) (clickHouseMigrationSession, error) {
				opened = true
				openedOptions = options
				if len(options.Addr) != 1 || options.Addr[0] != "clickhouse:9440" ||
					options.Auth.Database != "default" ||
					options.Auth.Username != deploymentClickHouseMigrationUsername ||
					options.Auth.Password != password || options.TLS == nil ||
					options.TLS.ServerName != "clickhouse" ||
					options.TLS.InsecureSkipVerify || options.MaxOpenConns != 1 ||
					options.MaxIdleConns != 1 {
					t.Fatalf("deployment migration connection options = %#v", options)
				}
				return connection, nil
			},
			apply: func(
				ctx context.Context,
				got server.ClickHouseMigrationConnection,
				migrationFiles fs.FS,
			) error {
				applied = true
				if ctx == nil || got != connection {
					t.Fatal("deployment migration received different runtime dependencies")
				}
				contents, readErr := fs.ReadFile(
					migrationFiles,
					"0001_baseline.sql",
				)
				if readErr != nil ||
					!strings.Contains(string(contents), "open_splunk.recovery_sets") {
					t.Fatalf(
						"deployment migration filesystem is not the embedded release: %v",
						readErr,
					)
				}
				return nil
			},
			validatePhysicalSchema: func(
				ctx context.Context,
				got server.ClickHouseVersionConnection,
			) error {
				if !applied {
					t.Fatal("deployment physical-schema validation ran before migrations")
				}
				validated = true
				if ctx == nil || got != connection {
					t.Fatal("deployment physical-schema validation received different dependencies")
				}
				return nil
			},
			waitUntilReady: func(
				context.Context,
				*clickhousedriver.Options,
				clickHouseMigrationOpener,
			) error {
				return nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !opened || !applied || !validated || connection.closeCalls != 1 {
		t.Fatalf(
			"deployment migration calls = (open %t, apply %t, validate %t, close %d), want (true, true, true, 1)",
			opened,
			applied,
			validated,
			connection.closeCalls,
		)
	}
	if openedOptions == nil || openedOptions.Auth.Password != "" {
		t.Fatal("deployment migration retained its loaded credential")
	}
	for _, name := range passwordEnvironments {
		if _, exists := os.LookupEnv(name); exists {
			t.Errorf("deployment migration retained password environment %s", name)
		}
	}
}

func TestDeploymentClickHouseMigrationValidatesPrivilegesBeforeDDL(t *testing.T) {
	identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	connection := &startupMigrationConnection{grants: []string{}}
	applied := false
	validated := false
	err = runDeploymentClickHouseMigrationWithDependencies(
		deploymentClickHouseMigrationOptions{
			Address:      "clickhouse:9440",
			PasswordFile: writeClickHouseCredentialFixture(t, "secret", 0o600),
			CACertFile:   identity.CertificateFile,
			ServerName:   "clickhouse",
		},
		deploymentClickHouseMigrationDependencies{
			migrationFiles: migrations.ClickHouse(),
			open: func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
				return connection, nil
			},
			apply: func(context.Context, server.ClickHouseMigrationConnection, fs.FS) error {
				applied = true
				return nil
			},
			validatePhysicalSchema: func(
				context.Context,
				server.ClickHouseVersionConnection,
			) error {
				validated = true
				return nil
			},
			waitUntilReady: func(
				context.Context,
				*clickhousedriver.Options,
				clickHouseMigrationOpener,
			) error {
				return nil
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "privilege") {
		t.Fatalf("deployment migration privilege error = %v", err)
	}
	if applied || validated || connection.closeCalls != 1 {
		t.Fatalf(
			"deployment migration failure calls = (apply %t, validate %t, close %d), want (false, false, 1)",
			applied,
			validated,
			connection.closeCalls,
		)
	}
}

func TestDeploymentClickHouseStableReadinessResetsAfterRestart(t *testing.T) {
	t.Parallel()

	results := []error{nil, nil, context.Canceled, nil, nil, nil}
	connections := make([]*startupMigrationConnection, 0, len(results))
	opens := 0
	err := waitForStableDeploymentClickHouseWithInterval(
		context.Background(),
		&clickhousedriver.Options{},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			probe := opens
			opens++
			connection := &startupMigrationConnection{
				ping: func(context.Context) error { return results[probe] },
			}
			connections = append(connections, connection)
			return connection, nil
		},
		time.Nanosecond,
		3,
	)
	if err != nil {
		t.Fatal(err)
	}
	if opens != len(results) {
		t.Fatalf("readiness probes = %d, want %d", opens, len(results))
	}
	for index, connection := range connections {
		if connection.closeCalls != 1 {
			t.Errorf(
				"readiness connection %d close calls = %d, want 1",
				index,
				connection.closeCalls,
			)
		}
	}
}

func TestDeploymentClickHouseMigrationRejectsUnsafeArguments(t *testing.T) {
	t.Parallel()

	for _, address := range []string{
		"", " clickhouse:9440", "clickhouse:9440 ", "click house:9440",
		"clickhouse", "clickhouse:0", "clickhouse:65536", "clickhouse:not-a-port",
	} {
		if _, err := validateDeploymentClickHouseMigrationAddress(address); err == nil {
			t.Errorf("deployment migration address %q succeeded", address)
		}
	}
	if _, err := validateDeploymentClickHouseMigrationAddress("clickhouse:9440"); err != nil {
		t.Fatalf("valid deployment migration address failed: %v", err)
	}

	for _, arguments := range [][]string{
		{"-unknown"},
		{"-address", "clickhouse:9440", "positional"},
	} {
		if _, err := parseDeploymentClickHouseMigrationOptions(arguments); err == nil {
			t.Errorf("deployment migration arguments %q succeeded", arguments)
		}
	}
}
