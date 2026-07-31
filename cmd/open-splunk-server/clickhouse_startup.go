package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

type clickHouseMigrationSession interface {
	server.ClickHouseMigrationConnection
	server.ClickHousePrivilegeConnection
	Ping(context.Context) error
	Close() error
}

type clickHouseMigrationOpener func(
	*clickhousedriver.Options,
) (clickHouseMigrationSession, error)

type clickHouseMigrationApplier func(
	context.Context,
	server.ClickHouseMigrationConnection,
	fs.FS,
) error

type clickHousePhysicalSchemaValidator func(
	context.Context,
	server.ClickHouseVersionConnection,
) error

func applyConfiguredStartupClickHouseMigrations(
	ctx context.Context,
	skip bool,
	options *clickhousedriver.Options,
	migrationFiles fs.FS,
	open clickHouseMigrationOpener,
	apply clickHouseMigrationApplier,
) error {
	if skip {
		return nil
	}
	return applyStartupClickHouseMigrations(
		ctx,
		options,
		migrationFiles,
		open,
		apply,
	)
}

// applyStartupClickHouseMigrations owns only the short-lived migration
// session. It closes that privileged connection before returning, so the
// long-lived Store, search, and deletion sessions can be opened afterward
// under their narrower identities.
func applyStartupClickHouseMigrations(
	ctx context.Context,
	options *clickhousedriver.Options,
	migrationFiles fs.FS,
	open clickHouseMigrationOpener,
	apply clickHouseMigrationApplier,
) (resultErr error) {
	return applyStartupClickHouseMigrationsWithPhysicalSchemaValidation(
		ctx,
		options,
		migrationFiles,
		open,
		apply,
		nil,
	)
}

// applyDeploymentClickHouseMigrations keeps the release-owned physical-schema
// proof on the same short-lived privileged session as its migrations. The
// deployment one-shot must not report success from a complete ledger while the
// events table is missing or was changed out of band.
func applyDeploymentClickHouseMigrations(
	ctx context.Context,
	options *clickhousedriver.Options,
	migrationFiles fs.FS,
	open clickHouseMigrationOpener,
	apply clickHouseMigrationApplier,
	validatePhysicalSchema clickHousePhysicalSchemaValidator,
) error {
	if validatePhysicalSchema == nil {
		return errors.New(
			"apply deployment ClickHouse migrations: physical-schema validator is required",
		)
	}
	return applyStartupClickHouseMigrationsWithPhysicalSchemaValidation(
		ctx,
		options,
		migrationFiles,
		open,
		apply,
		validatePhysicalSchema,
	)
}

func applyStartupClickHouseMigrationsWithPhysicalSchemaValidation(
	ctx context.Context,
	options *clickhousedriver.Options,
	migrationFiles fs.FS,
	open clickHouseMigrationOpener,
	apply clickHouseMigrationApplier,
	validatePhysicalSchema clickHousePhysicalSchemaValidator,
) (resultErr error) {
	if ctx == nil || options == nil || migrationFiles == nil ||
		open == nil || apply == nil {
		return errors.New(
			"apply startup ClickHouse migrations: context, options, filesystem, opener, and applier are required",
		)
	}
	connection, err := open(options)
	if err != nil {
		return fmt.Errorf("open ClickHouse migration session: %w", err)
	}
	if connection == nil {
		return errors.New(
			"open ClickHouse migration session: opener returned nil",
		)
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("close ClickHouse migration session: %w", closeErr),
			)
		}
	}()

	if err := connection.Ping(ctx); err != nil {
		return fmt.Errorf("ping ClickHouse migration session: %w", err)
	}
	if err := server.ValidateClickHouseMigrationPrivileges(ctx, connection); err != nil {
		return fmt.Errorf("validate ClickHouse migration session: %w", err)
	}
	if err := apply(ctx, connection, migrationFiles); err != nil {
		return fmt.Errorf("migrate ClickHouse: %w", err)
	}
	if validatePhysicalSchema != nil {
		if err := validatePhysicalSchema(ctx, connection); err != nil {
			return fmt.Errorf("validate migrated ClickHouse physical schema: %w", err)
		}
	}
	return nil
}
