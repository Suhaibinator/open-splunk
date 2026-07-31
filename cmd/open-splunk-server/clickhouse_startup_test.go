package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"testing"
	"testing/fstest"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

func TestApplyConfiguredStartupClickHouseMigrationsSkipsWithoutDependencies(
	t *testing.T,
) {
	t.Parallel()

	openCalls := 0
	applyCalls := 0
	err := applyConfiguredStartupClickHouseMigrations(
		context.Background(),
		true,
		nil,
		nil,
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			openCalls++
			return nil, errors.New("migration opener must not run")
		},
		func(
			context.Context,
			server.ClickHouseMigrationConnection,
			fs.FS,
		) error {
			applyCalls++
			return errors.New("migration applier must not run")
		},
	)
	if err != nil {
		t.Fatalf("applyConfiguredStartupClickHouseMigrations(skip) = %v", err)
	}
	if openCalls != 0 || applyCalls != 0 {
		t.Fatalf(
			"skipped migration calls = (open %d, apply %d), want zero",
			openCalls,
			applyCalls,
		)
	}
}

func TestApplyConfiguredStartupClickHouseMigrationsDefaultsToApplying(
	t *testing.T,
) {
	t.Parallel()

	connection := &startupMigrationConnection{}
	applyCalls := 0
	err := applyConfiguredStartupClickHouseMigrations(
		context.Background(),
		false,
		&clickhousedriver.Options{},
		fstest.MapFS{"migration.sql": &fstest.MapFile{}},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			return connection, nil
		},
		func(
			context.Context,
			server.ClickHouseMigrationConnection,
			fs.FS,
		) error {
			applyCalls++
			return nil
		},
	)
	if err != nil {
		t.Fatalf("applyConfiguredStartupClickHouseMigrations(default) = %v", err)
	}
	if applyCalls != 1 || connection.closeCalls != 1 {
		t.Fatalf(
			"default migration calls = (apply %d, close %d), want (1, 1)",
			applyCalls,
			connection.closeCalls,
		)
	}
}

func TestApplyStartupClickHouseMigrationsClosesBeforeReturning(t *testing.T) {
	t.Parallel()

	var events []string
	connection := &startupMigrationConnection{
		ping: func(context.Context) error {
			events = append(events, "ping")
			return nil
		},
		versionCheck: func() {
			events = append(events, "version")
		},
		close: func() error {
			events = append(events, "close")
			return nil
		},
	}
	options := &clickhousedriver.Options{
		Auth: clickhousedriver.Auth{Username: "migration-user"},
	}
	files := fstest.MapFS{"migration.sql": &fstest.MapFile{Data: []byte("SELECT 1")}}

	err := applyStartupClickHouseMigrations(
		context.Background(),
		options,
		files,
		func(opened *clickhousedriver.Options) (clickHouseMigrationSession, error) {
			events = append(events, "open")
			if opened != options {
				t.Fatal("migration opener received cloned or different options")
			}
			return connection, nil
		},
		func(
			_ context.Context,
			got server.ClickHouseMigrationConnection,
			gotFiles fs.FS,
		) error {
			events = append(events, "migrate")
			if got != connection || !reflect.DeepEqual(gotFiles, files) {
				t.Fatal("migration runner received different dependencies")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("applyStartupClickHouseMigrations(): %v", err)
	}
	if want := []string{"open", "ping", "version", "migrate", "close"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("startup migration events = %v, want %v", events, want)
	}
}

func TestApplyDeploymentClickHouseMigrationsRejectsPhysicalSchemaDriftBeforeClose(
	t *testing.T,
) {
	t.Parallel()

	var events []string
	connection := &startupMigrationConnection{
		ping: func(context.Context) error {
			events = append(events, "ping")
			return nil
		},
		versionCheck: func() {
			events = append(events, "version")
		},
		close: func() error {
			events = append(events, "close")
			return nil
		},
	}
	physicalSchemaErr := fmt.Errorf(
		"%w: injected mutation",
		server.ErrClickHousePhysicalSchemaDrift,
	)
	err := applyDeploymentClickHouseMigrations(
		context.Background(),
		&clickhousedriver.Options{},
		fstest.MapFS{"migration.sql": &fstest.MapFile{}},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			events = append(events, "open")
			return connection, nil
		},
		func(context.Context, server.ClickHouseMigrationConnection, fs.FS) error {
			events = append(events, "migrate")
			return nil
		},
		func(context.Context, server.ClickHouseVersionConnection) error {
			events = append(events, "validate")
			return physicalSchemaErr
		},
	)
	if !errors.Is(err, server.ErrClickHousePhysicalSchemaDrift) {
		t.Fatalf(
			"applyDeploymentClickHouseMigrations() error = %v, want physical drift",
			err,
		)
	}
	if want := []string{
		"open", "ping", "version", "migrate", "validate", "close",
	}; !reflect.DeepEqual(events, want) {
		t.Fatalf("deployment migration events = %v, want %v", events, want)
	}
}

func TestApplyDeploymentClickHouseMigrationsRequiresPhysicalSchemaValidator(
	t *testing.T,
) {
	t.Parallel()

	openCalls := 0
	err := applyDeploymentClickHouseMigrations(
		context.Background(),
		&clickhousedriver.Options{},
		fstest.MapFS{"migration.sql": &fstest.MapFile{}},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			openCalls++
			return &startupMigrationConnection{}, nil
		},
		func(context.Context, server.ClickHouseMigrationConnection, fs.FS) error {
			return nil
		},
		nil,
	)
	if err == nil {
		t.Fatal("applyDeploymentClickHouseMigrations(nil validator) succeeded")
	}
	if openCalls != 0 {
		t.Fatalf("open calls with nil physical-schema validator = %d, want 0", openCalls)
	}
}

func TestApplyStartupClickHouseMigrationsClosesOnEveryFailure(t *testing.T) {
	t.Parallel()

	pingErr := errors.New("ping")
	migrationErr := errors.New("migration")
	closeErr := errors.New("close")
	tests := []struct {
		name      string
		pingErr   error
		applyErr  error
		closeErr  error
		wantError []error
	}{
		{
			name:      "ping and close",
			pingErr:   pingErr,
			closeErr:  closeErr,
			wantError: []error{pingErr, closeErr},
		},
		{
			name:      "migration and close",
			applyErr:  migrationErr,
			closeErr:  closeErr,
			wantError: []error{migrationErr, closeErr},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection := &startupMigrationConnection{
				ping: func(context.Context) error { return test.pingErr },
				close: func() error {
					return test.closeErr
				},
			}
			applyCalls := 0
			err := applyStartupClickHouseMigrations(
				context.Background(),
				&clickhousedriver.Options{},
				fstest.MapFS{"migration.sql": &fstest.MapFile{}},
				func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
					return connection, nil
				},
				func(
					context.Context,
					server.ClickHouseMigrationConnection,
					fs.FS,
				) error {
					applyCalls++
					return test.applyErr
				},
			)
			for _, want := range test.wantError {
				if !errors.Is(err, want) {
					t.Fatalf("error = %v, want errors.Is(%v)", err, want)
				}
			}
			if test.pingErr != nil && applyCalls != 0 {
				t.Fatalf("migration calls after ping failure = %d, want 0", applyCalls)
			}
			if test.pingErr == nil && applyCalls != 1 {
				t.Fatalf("migration calls = %d, want 1", applyCalls)
			}
			if connection.closeCalls != 1 {
				t.Fatalf("close calls = %d, want 1", connection.closeCalls)
			}
		})
	}
}

func TestApplyStartupClickHouseMigrationsRejectsUnsupportedVersionBeforeDDL(
	t *testing.T,
) {
	t.Parallel()

	connection := &startupMigrationConnection{version: "26.4.1.1"}
	applyCalls := 0
	err := applyStartupClickHouseMigrations(
		context.Background(),
		&clickhousedriver.Options{},
		fstest.MapFS{"migration.sql": &fstest.MapFile{}},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			return connection, nil
		},
		func(
			context.Context,
			server.ClickHouseMigrationConnection,
			fs.FS,
		) error {
			applyCalls++
			return nil
		},
	)
	if !errors.Is(err, server.ErrClickHouseVersionUnsupported) {
		t.Fatalf(
			"applyStartupClickHouseMigrations() error = %v, want unsupported version",
			err,
		)
	}
	if applyCalls != 0 {
		t.Fatalf("migration calls = %d, want 0", applyCalls)
	}
	if connection.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", connection.closeCalls)
	}
}

func TestApplyStartupClickHouseMigrationsRejectsPrivilegeDriftBeforeDDL(
	t *testing.T,
) {
	t.Parallel()

	connection := &startupMigrationConnection{grants: []string{}}
	applyCalls := 0
	err := applyStartupClickHouseMigrations(
		context.Background(),
		&clickhousedriver.Options{},
		fstest.MapFS{"migration.sql": &fstest.MapFile{}},
		func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
			return connection, nil
		},
		func(
			context.Context,
			server.ClickHouseMigrationConnection,
			fs.FS,
		) error {
			applyCalls++
			return nil
		},
	)
	if !errors.Is(err, server.ErrClickHousePrivilegeMissing) {
		t.Fatalf(
			"applyStartupClickHouseMigrations() error = %v, want missing privilege",
			err,
		)
	}
	if applyCalls != 0 {
		t.Fatalf("migration calls = %d, want 0", applyCalls)
	}
	if connection.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", connection.closeCalls)
	}
}

func TestApplyStartupClickHouseMigrationsRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	validOptions := &clickhousedriver.Options{}
	validFiles := fstest.MapFS{"migration.sql": &fstest.MapFile{}}
	validOpen := func(*clickhousedriver.Options) (clickHouseMigrationSession, error) {
		return &startupMigrationConnection{}, nil
	}
	validApply := func(
		context.Context,
		server.ClickHouseMigrationConnection,
		fs.FS,
	) error {
		return nil
	}
	tests := []struct {
		name    string
		ctx     context.Context
		options *clickhousedriver.Options
		files   fs.FS
		open    clickHouseMigrationOpener
		apply   clickHouseMigrationApplier
	}{
		{name: "nil context", options: validOptions, files: validFiles, open: validOpen, apply: validApply},
		{name: "nil options", ctx: context.Background(), files: validFiles, open: validOpen, apply: validApply},
		{name: "nil files", ctx: context.Background(), options: validOptions, open: validOpen, apply: validApply},
		{name: "nil opener", ctx: context.Background(), options: validOptions, files: validFiles, apply: validApply},
		{name: "nil applier", ctx: context.Background(), options: validOptions, files: validFiles, open: validOpen},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := applyStartupClickHouseMigrations(
				test.ctx,
				test.options,
				test.files,
				test.open,
				test.apply,
			); err == nil {
				t.Fatal("applyStartupClickHouseMigrations unexpectedly succeeded")
			}
		})
	}
}

type startupMigrationConnection struct {
	clickhousedriver.Conn
	ping         func(context.Context) error
	close        func() error
	version      string
	versionErr   error
	versionCheck func()
	closeCalls   int
	grants       []string
}

func (connection *startupMigrationConnection) Ping(ctx context.Context) error {
	if connection.ping == nil {
		return nil
	}
	return connection.ping(ctx)
}

func (connection *startupMigrationConnection) Close() error {
	connection.closeCalls++
	if connection.close == nil {
		return nil
	}
	return connection.close()
}

func (connection *startupMigrationConnection) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) clickhouserow.Row {
	switch query {
	case "SELECT version()":
		if connection.versionCheck != nil {
			connection.versionCheck()
		}
		version := connection.version
		if version == "" {
			version = "26.3.17.4"
		}
		return startupMigrationRow{
			value: version,
			err:   connection.versionErr,
		}
	case "SELECT name FROM system.server_settings LIMIT 1":
		return startupMigrationRow{
			err: &clickhousedriver.Exception{
				Code: 497,
				Name: "ACCESS_DENIED",
			},
		}
	default:
		return startupMigrationRow{
			err: fmt.Errorf("unexpected startup migration query %q", query),
		}
	}
}

func (connection *startupMigrationConnection) Query(
	_ context.Context,
	query string,
	_ ...any,
) (clickhouserow.Rows, error) {
	if query != "SHOW GRANTS" {
		return nil, fmt.Errorf(
			"unexpected startup migration query %q",
			query,
		)
	}
	grants := connection.grants
	if grants == nil {
		grants = startupMigrationGrants()
	}
	return &startupMigrationRows{
		values: append([]string(nil), grants...),
	}, nil
}

type startupMigrationRow struct {
	value string
	err   error
}

func (row startupMigrationRow) Err() error {
	return row.err
}

func (row startupMigrationRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 1 {
		return errors.New("startup migration row requires one destination")
	}
	destination, ok := destinations[0].(*string)
	if !ok {
		return errors.New("startup migration row requires a string destination")
	}
	*destination = row.value
	return nil
}

func (startupMigrationRow) ScanStruct(any) error {
	return errors.New("startup migration row does not support ScanStruct")
}

type startupMigrationRows struct {
	values []string
	index  int
}

func (rows *startupMigrationRows) Next() bool {
	if rows.index >= len(rows.values) {
		return false
	}
	rows.index++
	return true
}

func (rows *startupMigrationRows) Scan(destinations ...any) error {
	if len(destinations) != 1 {
		return errors.New("startup migration rows require one destination")
	}
	destination, ok := destinations[0].(*string)
	if !ok {
		return errors.New("startup migration rows require a string destination")
	}
	if rows.index == 0 || rows.index > len(rows.values) {
		return errors.New("startup migration rows have no current row")
	}
	*destination = rows.values[rows.index-1]
	return nil
}

func (*startupMigrationRows) ScanStruct(any) error {
	return errors.New("startup migration rows do not support ScanStruct")
}

func (*startupMigrationRows) ColumnTypes() []clickhouserow.ColumnType {
	return nil
}

func (*startupMigrationRows) Totals(...any) error {
	return nil
}

func (*startupMigrationRows) Columns() []string {
	return []string{"GRANTS"}
}

func (*startupMigrationRows) Close() error {
	return nil
}

func (*startupMigrationRows) Err() error {
	return nil
}

func (rows *startupMigrationRows) HasData() bool {
	return len(rows.values) != 0
}

func startupMigrationGrants() []string {
	return []string{
		"GRANT CREATE DATABASE, SHOW TABLES ON open_splunk.* TO principal",
		"GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX, CREATE TABLE ON open_splunk.events TO principal",
		"GRANT CREATE TABLE, INSERT, SELECT ON open_splunk.schema_migrations TO principal",
		"GRANT SELECT ON system.tables TO principal",
	}
}
