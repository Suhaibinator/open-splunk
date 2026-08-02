package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
)

const (
	deploymentRecoveryBackupUsername  = "open_splunk_backup"
	deploymentRecoveryRestoreUsername = "open_splunk_restore"
	deploymentRecoveryTimeout         = 30 * time.Minute
	deploymentRecoveryArchiveUID      = 101
	deploymentRecoveryArchiveGID      = 65532
	deploymentRecoveryArchiveRootMode = fs.FileMode(0o750) | os.ModeSetgid
	deploymentRecoveryArchiveMode     = fs.FileMode(0o640)
)

type deploymentRecoveryBackupOptions struct {
	DatabasePath           string
	MasterKeyPath          string
	AdministratorTokenPath string
	Destination            string
	ArchiveRoot            string
	Address                string
	PasswordFile           string
	CACertFile             string
	ServerName             string
}

type deploymentRecoveryVerifyOptions struct {
	Source      string
	ArchiveRoot string
}

type deploymentRecoveryRestoreOptions struct {
	Source                 string
	ArchiveRoot            string
	DatabasePath           string
	MasterKeyPath          string
	AdministratorTokenPath string
	Address                string
	PasswordFile           string
	CACertFile             string
	ServerName             string
}

type deploymentRecoverySession interface {
	server.ClickHousePrivilegeConnection
	server.ClickHouseRecoveryValidationConnection
	server.ClickHouseRecoveryReceiptConnection
	Ping(context.Context) error
	Close() error
}

type deploymentRecoveryDependencies struct {
	migrationFiles            fs.FS
	open                      func(*clickhousedriver.Options) (deploymentRecoverySession, error)
	acquireDatabaseLock       databaseLockAcquirer
	createRecoverySet         func(context.Context, recoveryset.CreateOptions) (recoveryset.Verification, error)
	verifyRecoverySet         func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error)
	preflightControlPlane     func(context.Context, controlbackup.RestoreOptions) error
	restoreControlPlane       func(context.Context, controlbackup.RestoreOptions) error
	validateBackupPrivileges  func(context.Context, server.ClickHousePrivilegeConnection) error
	validateRestorePrivileges func(context.Context, server.ClickHousePrivilegeConnection) error
	validateRestoreDisk       func(context.Context, server.ClickHouseRecoveryDiskConnection) error
	validateBackupSource      func(
		context.Context,
		server.ClickHouseRecoveryValidationConnection,
		fs.FS,
	) (server.ClickHouseRecoveryDatabaseInspection, error)
	inspectDatabase func(
		context.Context,
		server.ClickHouseRecoveryValidationConnection,
		fs.FS,
		string,
	) (server.ClickHouseRecoveryDatabaseInspection, error)
	readReceipt func(
		context.Context,
		server.ClickHouseRecoveryReceiptConnection,
		string,
		string,
		string,
	) (server.ClickHouseRecoveryReceipt, error)
	writeReceipt func(
		context.Context,
		server.ClickHouseRecoveryReceiptConnection,
		string,
		server.ClickHouseRecoveryReceipt,
	) (server.ClickHouseRecoveryReceipt, error)
	listRecoveryDatabases func(context.Context, deploymentRecoverySession) ([]string, error)
	newOperationUUID      func() (uuid.UUID, error)
	now                   func() time.Time
}

func defaultDeploymentRecoveryDependencies() deploymentRecoveryDependencies {
	return deploymentRecoveryDependencies{
		migrationFiles: migrations.ClickHouse(),
		open: func(options *clickhousedriver.Options) (deploymentRecoverySession, error) {
			return openClickHouse(options)
		},
		acquireDatabaseLock:       acquireServerLock,
		createRecoverySet:         recoveryset.Create,
		verifyRecoverySet:         recoveryset.Verify,
		preflightControlPlane:     controlbackup.PreflightRestore,
		restoreControlPlane:       controlbackup.Restore,
		validateBackupPrivileges:  server.ValidateClickHouseBackupPrivileges,
		validateRestorePrivileges: server.ValidateClickHouseRestorePrivileges,
		validateRestoreDisk:       server.ValidateClickHouseRecoveryDiskReadOnly,
		validateBackupSource:      server.ValidateClickHouseBackupSource,
		inspectDatabase:           server.InspectClickHouseRecoveryDatabase,
		readReceipt:               server.ReadClickHouseRecoveryReceipt,
		writeReceipt:              server.WriteClickHouseRecoveryReceipt,
		listRecoveryDatabases:     deploymentRecoveryDatabaseNamespace,
		newOperationUUID:          uuid.NewRandom,
		now:                       time.Now,
	}
}

func runBackupDeploymentRecoverySetSubcommand(arguments []string) error {
	options, err := parseBackupDeploymentRecoverySetOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return fmt.Errorf("backup deployment recovery set: %w", err)
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, deploymentRecoveryTimeout)
	defer cancel()
	return runBackupDeploymentRecoverySetWithDependencies(
		ctx,
		options,
		release,
		defaultDeploymentRecoveryDependencies(),
	)
}

func runVerifyDeploymentRecoverySetSubcommand(arguments []string) error {
	options, err := parseVerifyDeploymentRecoverySetOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return fmt.Errorf("verify deployment recovery set: %w", err)
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	return runVerifyDeploymentRecoverySetWithDependencies(
		ctx,
		options,
		release,
		defaultDeploymentRecoveryDependencies(),
	)
}

func runRestoreDeploymentRecoverySetSubcommand(arguments []string) error {
	options, err := parseRestoreDeploymentRecoverySetOptions(arguments)
	if err != nil {
		return err
	}
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		return fmt.Errorf("restore deployment recovery set: %w", err)
	}
	ctx, stop := recoveryCommandContext()
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, deploymentRecoveryTimeout)
	defer cancel()
	return runRestoreDeploymentRecoverySetWithDependencies(
		ctx,
		options,
		release,
		defaultDeploymentRecoveryDependencies(),
	)
}

func parseBackupDeploymentRecoverySetOptions(
	arguments []string,
) (deploymentRecoveryBackupOptions, error) {
	flags := flag.NewFlagSet("backup-deployment-recovery-set", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options deploymentRecoveryBackupOptions
	flags.StringVar(&options.DatabasePath, "control-db", "", "absolute stopped-server SQLite database path")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute matching server master-key path")
	flags.StringVar(&options.AdministratorTokenPath, "administrator-token-file", "", "absolute matching administrator-token path")
	flags.StringVar(&options.Destination, "destination", "", "absolute new private recovery-set directory")
	flags.StringVar(&options.ArchiveRoot, "archive-root", "", "absolute ClickHouse recovery archive root")
	registerDeploymentRecoveryClickHouseFlags(flags, &options.Address, &options.PasswordFile, &options.CACertFile, &options.ServerName, "backup")
	if err := flags.Parse(arguments); err != nil {
		return deploymentRecoveryBackupOptions{}, fmt.Errorf("backup deployment recovery set: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return deploymentRecoveryBackupOptions{}, errors.New("backup deployment recovery set: positional arguments are not allowed")
	}
	if err := validateDeploymentRecoveryPaths("backup deployment recovery set", []deploymentRecoveryPath{
		{name: "-control-db", value: options.DatabasePath},
		{name: "-master-key", value: options.MasterKeyPath},
		{name: "-administrator-token-file", value: options.AdministratorTokenPath},
		{name: "-destination", value: options.Destination},
		{name: "-archive-root", value: options.ArchiveRoot},
		{name: "-password-file", value: options.PasswordFile},
		{name: "-ca-cert", value: options.CACertFile},
	}); err != nil {
		return deploymentRecoveryBackupOptions{}, err
	}
	if err := validateDeploymentRecoveryConnectionFields("backup deployment recovery set", options.Address, options.ServerName); err != nil {
		return deploymentRecoveryBackupOptions{}, err
	}
	return options, nil
}

func parseVerifyDeploymentRecoverySetOptions(
	arguments []string,
) (deploymentRecoveryVerifyOptions, error) {
	flags := flag.NewFlagSet("verify-deployment-recovery-set", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options deploymentRecoveryVerifyOptions
	flags.StringVar(&options.Source, "source", "", "absolute private recovery-set directory")
	flags.StringVar(&options.ArchiveRoot, "archive-root", "", "absolute ClickHouse recovery archive root")
	if err := flags.Parse(arguments); err != nil {
		return deploymentRecoveryVerifyOptions{}, fmt.Errorf("verify deployment recovery set: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return deploymentRecoveryVerifyOptions{}, errors.New("verify deployment recovery set: positional arguments are not allowed")
	}
	if err := validateDeploymentRecoveryPaths("verify deployment recovery set", []deploymentRecoveryPath{
		{name: "-source", value: options.Source},
		{name: "-archive-root", value: options.ArchiveRoot},
	}); err != nil {
		return deploymentRecoveryVerifyOptions{}, err
	}
	return options, nil
}

func parseRestoreDeploymentRecoverySetOptions(
	arguments []string,
) (deploymentRecoveryRestoreOptions, error) {
	flags := flag.NewFlagSet("restore-deployment-recovery-set", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options deploymentRecoveryRestoreOptions
	flags.StringVar(&options.Source, "source", "", "absolute verified recovery-set directory")
	flags.StringVar(&options.ArchiveRoot, "archive-root", "", "absolute ClickHouse recovery archive root")
	flags.StringVar(&options.DatabasePath, "control-db", "", "absolute absent or exactly resumed SQLite database path")
	flags.StringVar(&options.MasterKeyPath, "master-key", "", "absolute absent or exactly resumed server master-key path")
	flags.StringVar(&options.AdministratorTokenPath, "administrator-token-file", "", "absolute absent or exactly resumed administrator-token path")
	registerDeploymentRecoveryClickHouseFlags(flags, &options.Address, &options.PasswordFile, &options.CACertFile, &options.ServerName, "restore")
	if err := flags.Parse(arguments); err != nil {
		return deploymentRecoveryRestoreOptions{}, fmt.Errorf("restore deployment recovery set: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return deploymentRecoveryRestoreOptions{}, errors.New("restore deployment recovery set: positional arguments are not allowed")
	}
	if err := validateDeploymentRecoveryPaths("restore deployment recovery set", []deploymentRecoveryPath{
		{name: "-source", value: options.Source},
		{name: "-archive-root", value: options.ArchiveRoot},
		{name: "-control-db", value: options.DatabasePath},
		{name: "-master-key", value: options.MasterKeyPath},
		{name: "-administrator-token-file", value: options.AdministratorTokenPath},
		{name: "-password-file", value: options.PasswordFile},
		{name: "-ca-cert", value: options.CACertFile},
	}); err != nil {
		return deploymentRecoveryRestoreOptions{}, err
	}
	if err := validateDeploymentRecoveryConnectionFields("restore deployment recovery set", options.Address, options.ServerName); err != nil {
		return deploymentRecoveryRestoreOptions{}, err
	}
	return options, nil
}

func registerDeploymentRecoveryClickHouseFlags(
	flags *flag.FlagSet,
	address *string,
	passwordFile *string,
	caCertFile *string,
	serverName *string,
	principal string,
) {
	flags.StringVar(address, "address", "", "ClickHouse native TLS address")
	flags.StringVar(passwordFile, "password-file", "", "read-only "+principal+"-principal password file")
	flags.StringVar(caCertFile, "ca-cert", "", "explicit certificate-only CA bundle")
	flags.StringVar(serverName, "server-name", "", "explicit ClickHouse TLS certificate name")
}

type deploymentRecoveryPath struct {
	name  string
	value string
}

type exactOnceStringFlag struct {
	value          string
	set            bool
	duplicateError string
}

func (value *exactOnceStringFlag) String() string {
	if value == nil {
		return ""
	}
	return value.value
}

func (value *exactOnceStringFlag) Set(input string) error {
	if value.set {
		if value.duplicateError != "" {
			return errors.New(value.duplicateError)
		}
		return errors.New("flag may be specified exactly once")
	}
	value.value = input
	value.set = true
	return nil
}

func validateDeploymentRecoveryPaths(operation string, paths []deploymentRecoveryPath) error {
	for _, item := range paths {
		if err := validateRecoveryCommandPath(item.name, item.value); err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}
	}
	return nil
}

func validateDeploymentRecoveryConnectionFields(operation, address, serverName string) error {
	if _, err := validateExactDeploymentClickHouseAddress(address); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if err := validateExactDeploymentTLSServerName(serverName); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

func deploymentRecoveryArchiveOwnership() recoveryset.ArchiveOwnershipPolicy {
	return recoveryset.ArchiveOwnershipPolicy{
		Root: recoveryset.ArchiveObjectOwnershipPolicy{
			UID:  deploymentRecoveryArchiveUID,
			GID:  deploymentRecoveryArchiveGID,
			Mode: deploymentRecoveryArchiveRootMode,
		},
		File: recoveryset.ArchiveObjectOwnershipPolicy{
			UID:  deploymentRecoveryArchiveUID,
			GID:  deploymentRecoveryArchiveGID,
			Mode: deploymentRecoveryArchiveMode,
		},
	}
}

func runBackupDeploymentRecoverySetWithDependencies(
	ctx context.Context,
	options deploymentRecoveryBackupOptions,
	release controlbackup.ReleaseIdentity,
	dependencies deploymentRecoveryDependencies,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("backup deployment recovery set: context is required")
	}
	if err := validateDeploymentRecoveryBackupDependencies(dependencies); err != nil {
		return err
	}
	lock, err := dependencies.acquireDatabaseLock(options.DatabasePath)
	if err != nil {
		return fmt.Errorf("backup deployment recovery set: require the server to be stopped: %w", err)
	}
	if lock == nil {
		return errors.New("backup deployment recovery set: server lock dependency returned no lock")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lock.Close()) }()
	if err := discardDeploymentRecoveryCredentialEnvironment(); err != nil {
		return err
	}

	createOptions := recoveryset.CreateOptions{
		DatabasePath:           options.DatabasePath,
		MasterKeyPath:          options.MasterKeyPath,
		AdministratorTokenPath: options.AdministratorTokenPath,
		Destination:            options.Destination,
		ArchiveRoot:            options.ArchiveRoot,
		ArchiveOwnership:       deploymentRecoveryArchiveOwnership(),
		Release:                release,
		NativeBackup: func(
			callbackContext context.Context,
			request recoveryset.NativeBackupRequest,
		) (recoveryset.NativeBackupIdentity, error) {
			return runDeploymentNativeBackupWithConnection(
				callbackContext,
				options,
				request,
				dependencies,
			)
		},
	}
	if _, err := dependencies.createRecoverySet(ctx, createOptions); err != nil {
		return fmt.Errorf("backup deployment recovery set: %w", err)
	}
	return nil
}

func runVerifyDeploymentRecoverySetWithDependencies(
	ctx context.Context,
	options deploymentRecoveryVerifyOptions,
	release controlbackup.ReleaseIdentity,
	dependencies deploymentRecoveryDependencies,
) error {
	if ctx == nil {
		return errors.New("verify deployment recovery set: context is required")
	}
	if dependencies.verifyRecoverySet == nil {
		return errors.New("verify deployment recovery set: verifier dependency is required")
	}
	_, err := dependencies.verifyRecoverySet(ctx, recoveryset.VerifyOptions{
		Source:           options.Source,
		ArchiveRoot:      options.ArchiveRoot,
		ArchiveOwnership: deploymentRecoveryArchiveOwnership(),
		Release:          release,
	})
	if err != nil {
		return fmt.Errorf("verify deployment recovery set: %w", err)
	}
	return nil
}

func runRestoreDeploymentRecoverySetWithDependencies(
	ctx context.Context,
	options deploymentRecoveryRestoreOptions,
	release controlbackup.ReleaseIdentity,
	dependencies deploymentRecoveryDependencies,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("restore deployment recovery set: context is required")
	}
	if err := validateDeploymentRecoveryRestoreDependencies(dependencies); err != nil {
		return err
	}
	lock, err := dependencies.acquireDatabaseLock(options.DatabasePath)
	if err != nil {
		return fmt.Errorf("restore deployment recovery set: require the server to be stopped: %w", err)
	}
	if lock == nil {
		return errors.New("restore deployment recovery set: server lock dependency returned no lock")
	}
	defer func() { returnedErr = errors.Join(returnedErr, lock.Close()) }()
	databaseLock, err := lock.fileForExactPath(options.DatabasePath + ".server.lock")
	if err != nil {
		return fmt.Errorf("restore deployment recovery set: bind control database lock: %w", err)
	}
	if err := discardDeploymentRecoveryCredentialEnvironment(); err != nil {
		return err
	}

	verifyOptions := recoveryset.VerifyOptions{
		Source:           options.Source,
		ArchiveRoot:      options.ArchiveRoot,
		ArchiveOwnership: deploymentRecoveryArchiveOwnership(),
		Release:          release,
	}
	verification, err := dependencies.verifyRecoverySet(ctx, verifyOptions)
	if err != nil {
		return fmt.Errorf("restore deployment recovery set: verify before mutation: %w", err)
	}
	controlRestoreOptions := controlbackup.RestoreOptions{
		Source:                 filepath.Join(options.Source, verification.Manifest.ControlPlane.Directory),
		DatabasePath:           options.DatabasePath,
		DatabaseLock:           databaseLock,
		MasterKeyPath:          options.MasterKeyPath,
		AdministratorTokenPath: options.AdministratorTokenPath,
		Release:                release,
		ExpectedRecoverySetID:  verification.Manifest.RecoverySetID,
		ExpectedManifestSHA256: verification.Manifest.ControlPlane.Manifest.SHA256,
	}
	if err := dependencies.preflightControlPlane(ctx, controlRestoreOptions); err != nil {
		return fmt.Errorf("restore deployment recovery set: preflight control plane before ClickHouse mutation: %w", err)
	}
	session, err := openDeploymentRecoverySession(
		ctx,
		options.Address,
		options.PasswordFile,
		options.CACertFile,
		options.ServerName,
		deploymentRecoveryRestoreUsername,
		dependencies.open,
	)
	if err != nil {
		return fmt.Errorf("restore deployment recovery set: %w", err)
	}
	sessionOpen := true
	defer func() {
		if sessionOpen {
			if closeErr := session.Close(); closeErr != nil {
				returnedErr = errors.Join(
					returnedErr,
					fmt.Errorf("close ClickHouse restore session: %w", closeErr),
				)
			}
		}
	}()
	if err := dependencies.validateRestorePrivileges(ctx, session); err != nil {
		return fmt.Errorf("restore deployment recovery set: validate ClickHouse restore principal: %w", err)
	}
	if err := runDeploymentNativeRestore(
		ctx,
		session,
		verification,
		func(callbackContext context.Context) (recoveryset.Verification, error) {
			return dependencies.verifyRecoverySet(callbackContext, verifyOptions)
		},
		dependencies,
	); err != nil {
		return fmt.Errorf("restore deployment recovery set: %w", err)
	}
	if err := session.Close(); err != nil {
		sessionOpen = false
		return fmt.Errorf("restore deployment recovery set: close completed ClickHouse restore session: %w", err)
	}
	sessionOpen = false
	if err := dependencies.restoreControlPlane(ctx, controlRestoreOptions); err != nil {
		return fmt.Errorf("restore deployment recovery set control-plane member: %w", err)
	}
	return nil
}

func runDeploymentNativeBackupWithConnection(
	ctx context.Context,
	options deploymentRecoveryBackupOptions,
	request recoveryset.NativeBackupRequest,
	dependencies deploymentRecoveryDependencies,
) (identity recoveryset.NativeBackupIdentity, returnedErr error) {
	session, err := openDeploymentRecoverySession(
		ctx,
		options.Address,
		options.PasswordFile,
		options.CACertFile,
		options.ServerName,
		deploymentRecoveryBackupUsername,
		dependencies.open,
	)
	if err != nil {
		return recoveryset.NativeBackupIdentity{}, err
	}
	defer func() {
		if closeErr := session.Close(); closeErr != nil {
			returnedErr = errors.Join(returnedErr, fmt.Errorf("close ClickHouse backup session: %w", closeErr))
		}
	}()
	if err := dependencies.validateBackupPrivileges(ctx, session); err != nil {
		return recoveryset.NativeBackupIdentity{}, fmt.Errorf("validate ClickHouse backup principal: %w", err)
	}
	return runDeploymentNativeBackup(ctx, session, dependencies.migrationFiles, request, dependencies)
}

func openDeploymentRecoverySession(
	ctx context.Context,
	address string,
	passwordFile string,
	caCertFile string,
	serverName string,
	username string,
	open func(*clickhousedriver.Options) (deploymentRecoverySession, error),
) (deploymentRecoverySession, error) {
	if ctx == nil || open == nil {
		return nil, errors.New("open ClickHouse recovery session: context and opener are required")
	}
	validatedAddress, err := validateDeploymentClickHouseMigrationAddress(address)
	if err != nil {
		return nil, err
	}
	tlsProfile, err := loadClickHouseClientTLSProfile(true, caCertFile, serverName)
	if err != nil {
		return nil, fmt.Errorf("load ClickHouse recovery TLS identity: %w", err)
	}
	password, err := readClickHouseCredentialFile(passwordFile)
	if err != nil {
		return nil, fmt.Errorf("load ClickHouse recovery password file: %w", err)
	}
	defer clear(password)
	connectionOptions := &clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{validatedAddress},
		Auth: clickhousedriver.Auth{
			Database: "default",
			Username: username,
			Password: string(password),
		},
		TLS:              tlsProfile.newConfig(),
		DialTimeout:      5 * time.Second,
		ReadTimeout:      30 * time.Minute,
		MaxOpenConns:     1,
		MaxIdleConns:     1,
		ConnOpenStrategy: clickhousedriver.ConnOpenInOrder,
	}
	session, err := open(connectionOptions)
	connectionOptions.Auth.Password = ""
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse recovery session: %w", err)
	}
	if session == nil {
		return nil, errors.New("open ClickHouse recovery session: opener returned nil")
	}
	if err := session.Ping(ctx); err != nil {
		return nil, errors.Join(
			fmt.Errorf("ping ClickHouse recovery session: %w", err),
			session.Close(),
		)
	}
	return session, nil
}

func discardDeploymentRecoveryCredentialEnvironment() error {
	for _, name := range []string{
		"CLICKHOUSE_PASSWORD",
		clickHouseMigrationPasswordEnvironment,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
	} {
		if err := os.Unsetenv(name); err != nil {
			return fmt.Errorf("deployment recovery: discard password environment %s: %w", name, err)
		}
	}
	return nil
}

func validateDeploymentRecoveryBackupDependencies(dependencies deploymentRecoveryDependencies) error {
	if dependencies.migrationFiles == nil || dependencies.open == nil ||
		dependencies.acquireDatabaseLock == nil || dependencies.createRecoverySet == nil ||
		dependencies.validateBackupPrivileges == nil || dependencies.validateBackupSource == nil ||
		dependencies.newOperationUUID == nil {
		return errors.New("backup deployment recovery set: incomplete dependencies")
	}
	return nil
}

func validateDeploymentRecoveryRestoreDependencies(dependencies deploymentRecoveryDependencies) error {
	if dependencies.migrationFiles == nil || dependencies.open == nil ||
		dependencies.acquireDatabaseLock == nil || dependencies.verifyRecoverySet == nil ||
		dependencies.preflightControlPlane == nil || dependencies.restoreControlPlane == nil ||
		dependencies.validateRestorePrivileges == nil || dependencies.validateRestoreDisk == nil ||
		dependencies.inspectDatabase == nil || dependencies.readReceipt == nil ||
		dependencies.writeReceipt == nil || dependencies.listRecoveryDatabases == nil ||
		dependencies.newOperationUUID == nil || dependencies.now == nil {
		return errors.New("restore deployment recovery set: incomplete dependencies")
	}
	return nil
}

// A valid restore namespace contains at most one database. The second row is
// an overflow sentinel: once two reserved names exist, the restore must fail
// regardless of whether additional names were truncated by the bounded query.
const deploymentRecoveryDatabaseNamespaceScanLimit = 2

type deploymentRecoveryDatabaseNameRow struct {
	Name string `ch:"name"`
}

func deploymentRecoveryDatabaseNamespace(
	ctx context.Context,
	session deploymentRecoverySession,
) ([]string, error) {
	if ctx == nil || session == nil {
		return nil, errors.New(
			"inspect ClickHouse recovery database namespace: context and session are required",
		)
	}
	var rows []deploymentRecoveryDatabaseNameRow
	if err := session.Select(
		ctx,
		&rows,
		"SELECT name FROM system.databases WHERE startsWith(name, ?) ORDER BY name LIMIT 2",
		deploymentRecoveryDatabase,
	); err != nil {
		return nil, fmt.Errorf("inspect ClickHouse recovery database namespace: %w", err)
	}
	if len(rows) > deploymentRecoveryDatabaseNamespaceScanLimit {
		return nil, fmt.Errorf(
			"inspect ClickHouse recovery database namespace: query returned %d rows, bounded scan permits at most %d",
			len(rows),
			deploymentRecoveryDatabaseNamespaceScanLimit,
		)
	}
	names := make([]string, len(rows))
	for index, row := range rows {
		if !strings.HasPrefix(row.Name, deploymentRecoveryDatabase) {
			return nil, fmt.Errorf(
				"inspect ClickHouse recovery database namespace: row %d has unexpected name %q",
				index,
				row.Name,
			)
		}
		if index > 0 && rows[index-1].Name >= row.Name {
			return nil, errors.New(
				"inspect ClickHouse recovery database namespace: names are not strictly sorted and unique",
			)
		}
		names[index] = row.Name
	}
	return names, nil
}

var _ deploymentRecoverySession = (clickhousedriver.Conn)(nil)
