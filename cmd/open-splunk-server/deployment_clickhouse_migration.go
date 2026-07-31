package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	deploymentClickHouseMigrationUsername             = "open_splunk_migrator"
	deploymentClickHouseMigrationTimeout              = 10 * time.Minute
	deploymentClickHouseStableReadinessTimeout        = 45 * time.Second
	deploymentClickHouseStableReadinessInterval       = time.Second
	deploymentClickHouseStableReadinessRequiredProbes = 6
)

type deploymentClickHouseMigrationOptions struct {
	Address      string
	PasswordFile string
	CACertFile   string
	ServerName   string
}

type deploymentClickHouseMigrationDependencies struct {
	migrationFiles         fs.FS
	open                   clickHouseMigrationOpener
	apply                  clickHouseMigrationApplier
	validatePhysicalSchema clickHousePhysicalSchemaValidator
	waitUntilReady         func(
		context.Context,
		*clickhousedriver.Options,
		clickHouseMigrationOpener,
	) error
}

func runDeploymentClickHouseMigrationSubcommand(arguments []string) error {
	options, err := parseDeploymentClickHouseMigrationOptions(arguments)
	if err != nil {
		return err
	}
	return runDeploymentClickHouseMigration(options)
}

func parseDeploymentClickHouseMigrationOptions(
	arguments []string,
) (deploymentClickHouseMigrationOptions, error) {
	flags := flag.NewFlagSet("migrate-clickhouse", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options deploymentClickHouseMigrationOptions
	flags.StringVar(&options.Address, "address", "", "ClickHouse native TLS address")
	flags.StringVar(
		&options.PasswordFile,
		"password-file",
		"",
		"read-only migration-principal password file",
	)
	flags.StringVar(&options.CACertFile, "ca-cert", "", "explicit certificate-only CA bundle")
	flags.StringVar(&options.ServerName, "server-name", "", "explicit ClickHouse TLS certificate name")
	if err := flags.Parse(arguments); err != nil {
		return deploymentClickHouseMigrationOptions{}, fmt.Errorf(
			"deployment ClickHouse migration: parse flags: %w",
			err,
		)
	}
	if flags.NArg() != 0 {
		return deploymentClickHouseMigrationOptions{}, errors.New(
			"deployment ClickHouse migration: positional arguments are not allowed",
		)
	}
	return options, nil
}

func runDeploymentClickHouseMigration(
	options deploymentClickHouseMigrationOptions,
) error {
	return runDeploymentClickHouseMigrationWithDependencies(
		options,
		deploymentClickHouseMigrationDependencies{
			migrationFiles: migrations.ClickHouse(),
			open: func(
				options *clickhousedriver.Options,
			) (clickHouseMigrationSession, error) {
				return openClickHouse(options)
			},
			apply:                  server.ApplyClickHouseMigrations,
			validatePhysicalSchema: server.ValidateClickHousePhysicalSchema,
			waitUntilReady:         waitForStableDeploymentClickHouse,
		},
	)
}

func runDeploymentClickHouseMigrationWithDependencies(
	options deploymentClickHouseMigrationOptions,
	dependencies deploymentClickHouseMigrationDependencies,
) error {
	// The production command accepts the migrator secret only through its
	// bounded file. Clear every stray ClickHouse credential before any driver
	// is opened, even when this subcommand is invoked outside Compose.
	for _, environmentName := range []string{
		"CLICKHOUSE_PASSWORD",
		clickHouseMigrationPasswordEnvironment,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
	} {
		if err := os.Unsetenv(environmentName); err != nil {
			return fmt.Errorf(
				"deployment ClickHouse migration: discard password environment %s: %w",
				environmentName,
				err,
			)
		}
	}
	if dependencies.migrationFiles == nil || dependencies.open == nil ||
		dependencies.apply == nil || dependencies.validatePhysicalSchema == nil ||
		dependencies.waitUntilReady == nil {
		return errors.New(
			"deployment ClickHouse migration: migration filesystem, opener, applier, physical-schema validator, and readiness gate are required",
		)
	}

	address, err := validateDeploymentClickHouseMigrationAddress(options.Address)
	if err != nil {
		return err
	}
	tlsProfile, err := loadClickHouseClientTLSProfile(
		true,
		options.CACertFile,
		options.ServerName,
	)
	if err != nil {
		return fmt.Errorf("deployment ClickHouse migration: %w", err)
	}
	password, err := readClickHouseCredentialFile(options.PasswordFile)
	if err != nil {
		return fmt.Errorf(
			"deployment ClickHouse migration: load password file: %w",
			err,
		)
	}
	defer clear(password)

	connectionOptions := &clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{address},
		Auth: clickhousedriver.Auth{
			// The first embedded migration creates the application database.
			Database: "default",
			Username: deploymentClickHouseMigrationUsername,
			Password: string(password),
		},
		TLS:              tlsProfile.newConfig(),
		DialTimeout:      5 * time.Second,
		ReadTimeout:      30 * time.Second,
		MaxOpenConns:     1,
		MaxIdleConns:     1,
		ConnOpenStrategy: clickhousedriver.ConnOpenInOrder,
	}
	defer func() {
		connectionOptions.Auth.Password = ""
	}()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		deploymentClickHouseMigrationTimeout,
	)
	defer cancel()
	if err := dependencies.waitUntilReady(
		ctx,
		connectionOptions,
		dependencies.open,
	); err != nil {
		return fmt.Errorf("deployment ClickHouse migration: wait for stable server: %w", err)
	}
	if err := applyDeploymentClickHouseMigrations(
		ctx,
		connectionOptions,
		dependencies.migrationFiles,
		dependencies.open,
		dependencies.apply,
		dependencies.validatePhysicalSchema,
	); err != nil {
		return fmt.Errorf("deployment ClickHouse migration: %w", err)
	}
	return nil
}

// waitForStableDeploymentClickHouse prevents the official image's temporary
// initialization server from winning the Compose health race. That server is
// intentionally stopped before the durable server starts. Requiring a full
// window of fresh authenticated TLS sessions makes that transition reset the
// gate instead of interrupting release-coupled DDL.
func waitForStableDeploymentClickHouse(
	ctx context.Context,
	options *clickhousedriver.Options,
	open clickHouseMigrationOpener,
) error {
	readinessContext, cancel := context.WithTimeout(
		ctx,
		deploymentClickHouseStableReadinessTimeout,
	)
	defer cancel()
	return waitForStableDeploymentClickHouseWithInterval(
		readinessContext,
		options,
		open,
		deploymentClickHouseStableReadinessInterval,
		deploymentClickHouseStableReadinessRequiredProbes,
	)
}

func waitForStableDeploymentClickHouseWithInterval(
	ctx context.Context,
	options *clickhousedriver.Options,
	open clickHouseMigrationOpener,
	interval time.Duration,
	requiredConsecutiveProbes int,
) error {
	if ctx == nil || options == nil || open == nil || interval <= 0 ||
		requiredConsecutiveProbes < 2 {
		return errors.New(
			"wait for stable deployment ClickHouse: context, options, opener, positive interval, and at least two probes are required",
		)
	}
	consecutive := 0
	var lastProbeErr error
	for {
		probeErr := probeDeploymentClickHouse(ctx, options, open)
		if probeErr == nil {
			consecutive++
			if consecutive == requiredConsecutiveProbes {
				return nil
			}
		} else {
			consecutive = 0
			lastProbeErr = probeErr
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return errors.Join(ctx.Err(), lastProbeErr)
		case <-timer.C:
		}
	}
}

func probeDeploymentClickHouse(
	ctx context.Context,
	options *clickhousedriver.Options,
	open clickHouseMigrationOpener,
) (resultErr error) {
	connection, err := open(options)
	if err != nil {
		return fmt.Errorf("open readiness session: %w", err)
	}
	if connection == nil {
		return errors.New("open readiness session: opener returned nil")
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("close readiness session: %w", closeErr),
			)
		}
	}()
	if err := connection.Ping(ctx); err != nil {
		return fmt.Errorf("ping readiness session: %w", err)
	}
	if err := server.ValidateClickHouseServerVersion(ctx, connection); err != nil {
		return fmt.Errorf("validate readiness session: %w", err)
	}
	return nil
}

func validateDeploymentClickHouseMigrationAddress(rawAddress string) (string, error) {
	if rawAddress == "" || rawAddress != strings.TrimSpace(rawAddress) ||
		strings.IndexByte(rawAddress, 0) >= 0 {
		return "", errors.New(
			"deployment ClickHouse migration: -address must be an exact host:port address",
		)
	}
	host, port, err := net.SplitHostPort(rawAddress)
	if err != nil || host == "" || port == "" ||
		!validExplicitTLSServerName(host) {
		return "", errors.New(
			"deployment ClickHouse migration: -address must be an exact host:port address",
		)
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return "", errors.New(
			"deployment ClickHouse migration: -address port must be between 1 and 65535",
		)
	}
	return rawAddress, nil
}
