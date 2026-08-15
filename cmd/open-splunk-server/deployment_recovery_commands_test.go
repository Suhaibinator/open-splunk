//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
)

func TestRunDeploymentSubcommandRecognizesDeploymentRecoverySetCommands(t *testing.T) {
	t.Parallel()

	for _, command := range []string{
		"backup-deployment-recovery-set",
		"verify-deployment-recovery-set",
		"restore-deployment-recovery-set",
		"reconcile-deployment-recovery-marker",
	} {
		handled, err := runDeploymentSubcommand([]string{command, "-unknown"})
		if !handled || err == nil {
			t.Errorf("%s dispatch = (%t, %v), want (true, error)", command, handled, err)
		}
	}
}

func TestDeploymentRecoverySetFlagParsersRequireCompleteExactInputs(t *testing.T) {
	t.Parallel()

	root := filepath.Join(string(filepath.Separator), "private", "recovery")
	backupArguments := []string{
		"-control-db", filepath.Join(root, "open-splunk.db"),
		"-master-key", filepath.Join(root, "master.key"),
		"-administrator-token-file", filepath.Join(root, "administrator.token"),
		"-destination", filepath.Join(root, "set"),
		"-archive-root", filepath.Join(root, "archives"),
		"-address", "clickhouse:9440",
		"-password-file", filepath.Join(root, "backup.password"),
		"-ca-cert", filepath.Join(root, "clickhouse-ca.pem"),
		"-server-name", "clickhouse",
	}
	backup, err := parseBackupDeploymentRecoverySetOptions(backupArguments)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Address != "clickhouse:9440" ||
		backup.PasswordFile != filepath.Join(root, "backup.password") ||
		backup.CACertFile != filepath.Join(root, "clickhouse-ca.pem") ||
		backup.ServerName != "clickhouse" ||
		backup.DatabasePath != filepath.Join(root, "open-splunk.db") ||
		backup.MasterKeyPath != filepath.Join(root, "master.key") ||
		backup.AdministratorTokenPath != filepath.Join(root, "administrator.token") ||
		backup.Destination != filepath.Join(root, "set") ||
		backup.ArchiveRoot != filepath.Join(root, "archives") {
		t.Fatalf("parsed deployment recovery backup options = %#v", backup)
	}

	verify, err := parseVerifyDeploymentRecoverySetOptions([]string{
		"-source", filepath.Join(root, "set"),
		"-archive-root", filepath.Join(root, "archives"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if verify.Source != filepath.Join(root, "set") ||
		verify.ArchiveRoot != filepath.Join(root, "archives") {
		t.Fatalf("parsed deployment recovery verify options = %#v", verify)
	}

	restoreArguments := []string{
		"-source", filepath.Join(root, "set"),
		"-archive-root", filepath.Join(root, "archives"),
		"-control-db", filepath.Join(root, "restored.db"),
		"-master-key", filepath.Join(root, "restored.key"),
		"-administrator-token-file", filepath.Join(root, "restored.token"),
		"-address", "clickhouse:9440",
		"-password-file", filepath.Join(root, "restore.password"),
		"-ca-cert", filepath.Join(root, "clickhouse-ca.pem"),
		"-server-name", "clickhouse",
	}
	restore, err := parseRestoreDeploymentRecoverySetOptions(restoreArguments)
	if err != nil {
		t.Fatal(err)
	}
	if restore.Address != "clickhouse:9440" ||
		restore.PasswordFile != filepath.Join(root, "restore.password") ||
		restore.CACertFile != filepath.Join(root, "clickhouse-ca.pem") ||
		restore.ServerName != "clickhouse" ||
		restore.Source != filepath.Join(root, "set") ||
		restore.ArchiveRoot != filepath.Join(root, "archives") ||
		restore.DatabasePath != filepath.Join(root, "restored.db") ||
		restore.MasterKeyPath != filepath.Join(root, "restored.key") ||
		restore.AdministratorTokenPath != filepath.Join(root, "restored.token") {
		t.Fatalf("parsed deployment recovery restore options = %#v", restore)
	}

	for name, parse := range map[string]func() error{
		"backup missing": func() error {
			_, parseErr := parseBackupDeploymentRecoverySetOptions(nil)
			return parseErr
		},
		"backup positional": func() error {
			_, parseErr := parseBackupDeploymentRecoverySetOptions(append(backupArguments, "extra"))
			return parseErr
		},
		"verify missing": func() error {
			_, parseErr := parseVerifyDeploymentRecoverySetOptions(nil)
			return parseErr
		},
		"verify positional": func() error {
			_, parseErr := parseVerifyDeploymentRecoverySetOptions([]string{
				"-source", filepath.Join(root, "set"),
				"-archive-root", filepath.Join(root, "archives"),
				"extra",
			})
			return parseErr
		},
		"restore missing": func() error {
			_, parseErr := parseRestoreDeploymentRecoverySetOptions(nil)
			return parseErr
		},
		"restore positional": func() error {
			_, parseErr := parseRestoreDeploymentRecoverySetOptions(append(restoreArguments, "extra"))
			return parseErr
		},
	} {
		if err := parse(); err == nil {
			t.Errorf("%s parser succeeded", name)
		}
	}
}

func TestDeploymentRecoverySetFlagParsersRejectUnsafePathAddressAndTLSInputs(t *testing.T) {
	t.Parallel()

	root := filepath.Join(string(filepath.Separator), "private", "recovery")
	validBackup := []string{
		"-control-db", filepath.Join(root, "open-splunk.db"),
		"-master-key", filepath.Join(root, "master.key"),
		"-administrator-token-file", filepath.Join(root, "administrator.token"),
		"-destination", filepath.Join(root, "set"),
		"-archive-root", filepath.Join(root, "archives"),
		"-address", "clickhouse:9440",
		"-password-file", filepath.Join(root, "backup.password"),
		"-ca-cert", filepath.Join(root, "clickhouse-ca.pem"),
		"-server-name", "clickhouse",
	}
	for _, test := range []struct {
		name  string
		flag  string
		value string
	}{
		{name: "relative state", flag: "-control-db", value: "open-splunk.db"},
		{name: "unclean archive", flag: "-archive-root", value: root + "/x/../archives"},
		{name: "relative password", flag: "-password-file", value: "backup.password"},
		{name: "relative CA", flag: "-ca-cert", value: "clickhouse-ca.pem"},
		{name: "missing address port", flag: "-address", value: "clickhouse"},
		{name: "address whitespace", flag: "-address", value: " clickhouse:9440"},
		{name: "empty server name", flag: "-server-name", value: ""},
		{name: "server name whitespace", flag: "-server-name", value: "click house"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			arguments := append([]string(nil), validBackup...)
			for index := range arguments {
				if arguments[index] == test.flag {
					arguments[index+1] = test.value
					break
				}
			}
			if _, err := parseBackupDeploymentRecoverySetOptions(arguments); err == nil {
				t.Fatalf("backup parser accepted %s %q", test.flag, test.value)
			} else if strings.Contains(err.Error(), "migration") ||
				strings.Contains(err.Error(), "healthcheck") {
				t.Fatalf("backup recovery diagnostic leaked another command context: %v", err)
			}
		})
	}
}

func TestDeploymentRecoveryBackupLocksBeforeCredentialsStateOrNetwork(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("server is still running")
	accessed := false
	err := runBackupDeploymentRecoverySetWithDependencies(
		t.Context(),
		deploymentRecoveryBackupOptions{
			DatabasePath: "/missing/control.db",
			PasswordFile: "/missing/backup.password",
			CACertFile:   "/missing/ca.pem",
		},
		testRecoveryReleaseIdentity(),
		deploymentRecoveryDependencies{
			migrationFiles: migrations.ClickHouse(),
			acquireDatabaseLock: func(string) (*serverLock, error) {
				return nil, wantErr
			},
			open: func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
				accessed = true
				return nil, errors.New("network must not be opened")
			},
			createRecoverySet: func(
				context.Context,
				recoveryset.CreateOptions,
			) (recoveryset.Verification, error) {
				accessed = true
				return recoveryset.Verification{}, errors.New("state must not be read")
			},
			validateBackupPrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
				accessed = true
				return nil
			},
			validateBackupSource: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS) (server.ClickHouseRecoveryDatabaseInspection, error) {
				accessed = true
				return server.ClickHouseRecoveryDatabaseInspection{}, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				accessed = true
				return uuid.Nil, nil
			},
		},
	)
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("backup lock error = %v, want stopped-server wrapper", err)
	}
	if accessed {
		t.Fatal("backup accessed credentials, deployment state, or network before locking")
	}
}

func TestDeploymentRecoveryRestoreLocksBeforeVerificationCredentialsOrNetwork(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("server is still running")
	accessed := false
	err := runRestoreDeploymentRecoverySetWithDependencies(
		t.Context(),
		deploymentRecoveryRestoreOptions{
			DatabasePath: "/missing/control.db",
			Source:       "/missing/recovery-set",
			ArchiveRoot:  "/missing/archives",
			PasswordFile: "/missing/restore.password",
			CACertFile:   "/missing/ca.pem",
		},
		testRecoveryReleaseIdentity(),
		deploymentRecoveryDependencies{
			migrationFiles: migrations.ClickHouse(),
			acquireDatabaseLock: func(string) (*serverLock, error) {
				return nil, wantErr
			},
			verifyRecoverySet: func(
				context.Context,
				recoveryset.VerifyOptions,
			) (recoveryset.Verification, error) {
				accessed = true
				return recoveryset.Verification{}, errors.New("state must not be read")
			},
			open: func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
				accessed = true
				return nil, errors.New("network must not be opened")
			},
			preflightControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
				accessed = true
				return nil
			},
			restoreControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
				accessed = true
				return nil
			},
			validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
				accessed = true
				return nil
			},
			validateRestoreDisk: func(context.Context, server.ClickHouseRecoveryDiskConnection) error {
				accessed = true
				return nil
			},
			inspectDatabase: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS, string) (server.ClickHouseRecoveryDatabaseInspection, error) {
				accessed = true
				return server.ClickHouseRecoveryDatabaseInspection{}, nil
			},
			readReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, string, string) (server.ClickHouseRecoveryReceipt, error) {
				accessed = true
				return server.ClickHouseRecoveryReceipt{}, nil
			},
			writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
				accessed = true
				return server.ClickHouseRecoveryReceipt{}, nil
			},
			listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
				accessed = true
				return nil, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				accessed = true
				return uuid.Nil, nil
			},
			now: func() time.Time {
				accessed = true
				return time.Time{}
			},
		},
	)
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("restore lock error = %v, want stopped-server wrapper", err)
	}
	if accessed {
		t.Fatal("restore verified state, loaded credentials, or opened network before locking")
	}
}

func TestDeploymentRecoveryBackupUsesExactNativeOperationAndStableSource(t *testing.T) {
	fixture := newDeploymentRecoveryCommandFixture(t)
	operationID := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	recoverySetID := strings.Repeat("a", 32)
	wantInspection := deploymentRecoveryInspectionFixture()
	var order []string
	validations := 0
	created := false
	session := &deploymentRecoveryTestSession{
		markerOperation: func(operation, databaseName string) {
			if databaseName != deploymentRecoveryDatabase {
				t.Fatalf("backup marker database = %q", databaseName)
			}
			order = append(order, "marker-"+operation)
		},
		ping: func(context.Context) error {
			order = append(order, "ping")
			return nil
		},
		queryRow: func(_ context.Context, query string, args ...any) clickhouserow.Row {
			order = append(order, "native-backup")
			wantQuery := "BACKUP DATABASE open_splunk AS open_splunk_recovery_" + recoverySetID +
				" TO Disk('open_splunk_recovery', '" + recoverySetID +
				".tar.zst') SETTINGS id = '" + operationID.String() + "'"
			if query != wantQuery || len(args) != 0 {
				t.Fatalf("native backup query = %q args=%#v, want %q without arguments", query, args, wantQuery)
			}
			return deploymentRecoveryTestRow{values: []any{operationID.String(), "BACKUP_CREATED"}}
		},
	}

	for _, name := range deploymentRecoveryPasswordEnvironmentNamesForTest() {
		t.Setenv(name, "must-be-cleared")
	}
	err := runBackupDeploymentRecoverySetWithDependencies(
		t.Context(),
		fixture.backupOptions(),
		fixture.release,
		deploymentRecoveryDependencies{
			migrationFiles: migrations.ClickHouse(),
			acquireDatabaseLock: func(path string) (*serverLock, error) {
				order = append(order, "lock")
				if path != fixture.databasePath {
					t.Fatalf("lock database path = %q, want %q", path, fixture.databasePath)
				}
				return &serverLock{}, nil
			},
			open: func(options *clickhousedriver.Options) (deploymentRecoverySession, error) {
				order = append(order, "open")
				assertDeploymentRecoveryConnectionOptions(
					t,
					options,
					"open_splunk_backup",
					fixture.password,
				)
				return session, nil
			},
			validateBackupPrivileges: func(
				context.Context,
				server.ClickHousePrivilegeConnection,
			) error {
				order = append(order, "privileges")
				return nil
			},
			validateBackupSource: func(
				_ context.Context,
				got server.ClickHouseRecoveryValidationConnection,
				gotFS fs.FS,
			) (server.ClickHouseRecoveryDatabaseInspection, error) {
				validations++
				order = append(order, "source-"+string(rune('0'+validations)))
				if got != session || gotFS == nil {
					t.Fatal("backup source validator received different dependencies")
				}
				return wantInspection, nil
			},
			createRecoverySet: func(
				ctx context.Context,
				options recoveryset.CreateOptions,
			) (recoveryset.Verification, error) {
				order = append(order, "create")
				if options.DatabasePath != fixture.databasePath ||
					options.MasterKeyPath != fixture.masterKeyPath ||
					options.AdministratorTokenPath != fixture.administratorTokenPath ||
					options.Destination != fixture.destination ||
					options.ArchiveRoot != fixture.archiveRoot ||
					options.Release != fixture.release || options.NativeBackup == nil {
					t.Fatalf("recovery-set create options = %#v", options)
				}
				identity, createErr := options.NativeBackup(ctx, recoveryset.NativeBackupRequest{
					RecoverySetID:   recoverySetID,
					Disk:            "open_splunk_recovery",
					Database:        "open_splunk",
					ArchiveDatabase: "open_splunk_recovery_" + recoverySetID,
					ArchiveName:     recoverySetID + ".tar.zst",
					ArchivePath:     filepath.Join(fixture.archiveRoot, recoverySetID+".tar.zst"),
				})
				if createErr != nil {
					return recoveryset.Verification{}, createErr
				}
				assertNativeBackupIdentityMatchesInspection(t, identity, wantInspection, operationID)
				created = true
				return recoveryset.Verification{}, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				order = append(order, "uuid")
				return operationID, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"lock", "create", "open", "ping", "privileges", "source-1", "uuid",
		"marker-read", "marker-insert", "marker-read", "native-backup", "marker-read",
		"source-2", "marker-read", "marker-truncate", "marker-read",
	}
	if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("backup order = %v, want %v", order, wantOrder)
	}
	if !created || validations != 2 || session.closeCalls != 1 {
		t.Fatalf("backup calls = created:%t validations:%d closes:%d", created, validations, session.closeCalls)
	}
	for _, name := range deploymentRecoveryPasswordEnvironmentNamesForTest() {
		if _, exists := os.LookupEnv(name); exists {
			t.Errorf("backup retained password environment %s", name)
		}
	}
}

type deploymentRecoveryCommandFixture struct {
	databasePath           string
	masterKeyPath          string
	administratorTokenPath string
	destination            string
	archiveRoot            string
	passwordFile           string
	caCertFile             string
	password               string
	release                controlbackup.ReleaseIdentity
}

func newDeploymentRecoveryCommandFixture(t *testing.T) deploymentRecoveryCommandFixture {
	t.Helper()
	root := t.TempDir()
	identity, err := testsupport.WriteServerTLSIdentity(root, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	const password = "one-shot-recovery-password"
	return deploymentRecoveryCommandFixture{
		databasePath:           filepath.Join(root, "control.db"),
		masterKeyPath:          filepath.Join(root, "master.key"),
		administratorTokenPath: filepath.Join(root, "administrator.token"),
		destination:            filepath.Join(root, "deployment-recovery-set"),
		archiveRoot:            filepath.Join(root, "archives"),
		passwordFile:           writeClickHouseCredentialFixture(t, password+"\n", 0o600),
		caCertFile:             identity.CertificateFile,
		password:               password,
		release:                testRecoveryReleaseIdentity(),
	}
}

func acquireDeploymentRecoveryTestServerLock(
	t *testing.T,
	databasePath string,
) (*serverLock, error) {
	t.Helper()
	return acquireServerLockAt(
		databasePath,
		filepath.Join(t.TempDir(), "host.server.lock"),
	)
}

func createDeploymentControlBackupFixture(
	t *testing.T,
	fixture *deploymentRecoveryCommandFixture,
) (controlbackup.Manifest, string) {
	t.Helper()
	root := filepath.Dir(fixture.destination)
	if err := os.Mkdir(fixture.destination, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceDirectory := filepath.Join(root, "source-control")
	if err := os.Mkdir(sourceDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceDatabasePath := filepath.Join(sourceDirectory, "control.db")
	sourceDatabase, err := control.Open(t.Context(), sourceDatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	masterKey := bytes.Repeat([]byte{0x4d}, auth.ServerMasterKeyBytes)
	fingerprint, err := auth.FingerprintServerMasterKey(masterKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RegisterServerMasterKeyIdentity(
		t.Context(),
		sourceDatabase,
		fingerprint[:],
	); err != nil {
		t.Fatal(err)
	}
	migrationIdentity, err := sourceDatabase.VerifyCurrentMigrations(
		t.Context(),
		migrations.SQLite(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceDatabase.Close(); err != nil {
		t.Fatal(err)
	}
	sourceMasterKeyPath := filepath.Join(sourceDirectory, "master.key")
	if err := os.WriteFile(sourceMasterKeyPath, masterKey, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceAdministratorTokenPath := filepath.Join(sourceDirectory, "administrator.token")
	if err := os.WriteFile(
		sourceAdministratorTokenPath,
		bytes.Repeat([]byte{'R'}, auth.MinimumBrowserBearerTokenBytes),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	fixture.release.SQLiteMigrations = controlbackup.MigrationIdentity{
		SHA256:        hex.EncodeToString(migrationIdentity.SHA256[:]),
		LatestVersion: migrationIdentity.LatestVersion,
	}
	manifest, err := controlbackup.Create(t.Context(), controlbackup.CreateOptions{
		DatabasePath:           sourceDatabasePath,
		MasterKeyPath:          sourceMasterKeyPath,
		AdministratorTokenPath: sourceAdministratorTokenPath,
		Destination:            filepath.Join(fixture.destination, "control-plane"),
		Release:                fixture.release,
	})
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(
		fixture.destination,
		"control-plane",
		"manifest.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	return manifest, hex.EncodeToString(digest[:])
}

func bindDeploymentVerificationToControlBackup(
	verification recoveryset.Verification,
	manifest controlbackup.Manifest,
	manifestSHA256 string,
) recoveryset.Verification {
	recoverySetID := manifest.RecoverySetID
	verification.Manifest.RecoverySetID = recoverySetID
	verification.Manifest.ControlPlane.Manifest.SHA256 = manifestSHA256
	verification.Manifest.ClickHouse.ArchiveDatabase =
		deploymentRecoveryArchiveDatabasePrefix + recoverySetID
	verification.Manifest.ClickHouse.Archive.Name = recoverySetID + deploymentRecoveryArchiveSuffix
	return verification
}

func (fixture deploymentRecoveryCommandFixture) backupOptions() deploymentRecoveryBackupOptions {
	return deploymentRecoveryBackupOptions{
		DatabasePath:           fixture.databasePath,
		MasterKeyPath:          fixture.masterKeyPath,
		AdministratorTokenPath: fixture.administratorTokenPath,
		Destination:            fixture.destination,
		ArchiveRoot:            fixture.archiveRoot,
		Address:                "clickhouse:9440",
		PasswordFile:           fixture.passwordFile,
		CACertFile:             fixture.caCertFile,
		ServerName:             "clickhouse",
	}
}

func (fixture deploymentRecoveryCommandFixture) restoreOptions() deploymentRecoveryRestoreOptions {
	return deploymentRecoveryRestoreOptions{
		Source:                 fixture.destination,
		ArchiveRoot:            fixture.archiveRoot,
		DatabasePath:           fixture.databasePath,
		MasterKeyPath:          fixture.masterKeyPath,
		AdministratorTokenPath: fixture.administratorTokenPath,
		Address:                "clickhouse:9440",
		PasswordFile:           fixture.passwordFile,
		CACertFile:             fixture.caCertFile,
		ServerName:             "clickhouse",
	}
}

func deploymentRecoveryPasswordEnvironmentNamesForTest() []string {
	return []string{
		"CLICKHOUSE_PASSWORD",
		clickHouseMigrationPasswordEnvironment,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
	}
}

func deploymentRecoveryInspectionFixture() server.ClickHouseRecoveryDatabaseInspection {
	ledgerDigest := sha256.Sum256([]byte("release-owned migration ledger"))
	return server.ClickHouseRecoveryDatabaseInspection{
		DatabaseName:                    "open_splunk",
		ServerVersion:                   "26.7.3.19",
		DatabaseEngine:                  "Atomic",
		DatabaseUUID:                    "11111111-1111-4111-8111-111111111111",
		SchemaMigrationsTableUUID:       "22222222-2222-4222-8222-222222222222",
		EventsTableUUID:                 "33333333-3333-4333-8333-333333333333",
		RecoverySetsTableUUID:           "44444444-4444-4444-8444-444444444444",
		RecoveryArchiveMarkersTableUUID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		MaximumVisibilitySequence:       42,
		ActiveMutationCount:             0,
		MigrationLedger: server.ClickHouseMigrationLedgerIdentity{
			LatestVersion: 1,
			SHA256:        ledgerDigest,
		},
	}
}

func deploymentRestoredInspectionFixture(
	databaseName string,
) server.ClickHouseRecoveryDatabaseInspection {
	inspection := deploymentRecoveryInspectionFixture()
	inspection.DatabaseName = databaseName
	// RESTORE DATABASE ... AS intentionally generates fresh target UUIDs. The
	// durable receipt binds these restored UUIDs for crash-safe retries.
	inspection.DatabaseUUID = "77777777-7777-4777-8777-777777777777"
	inspection.SchemaMigrationsTableUUID = "88888888-8888-4888-8888-888888888888"
	inspection.EventsTableUUID = "99999999-9999-4999-8999-999999999999"
	inspection.RecoverySetsTableUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	inspection.RecoveryArchiveMarkersTableUUID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	return inspection
}

func assertDeploymentRecoveryConnectionOptions(
	t *testing.T,
	options *clickhousedriver.Options,
	username string,
	password string,
) {
	t.Helper()
	if options == nil || len(options.Addr) != 1 || options.Addr[0] != "clickhouse:9440" ||
		options.Protocol != clickhousedriver.Native || options.Auth.Database != "default" ||
		options.Auth.Username != username || options.Auth.Password != password ||
		options.TLS == nil || options.TLS.ServerName != "clickhouse" ||
		options.TLS.InsecureSkipVerify || options.MaxOpenConns != 1 ||
		options.MaxIdleConns != 1 {
		t.Fatalf("deployment recovery connection options = %#v", options)
	}
}

func assertNativeBackupIdentityMatchesInspection(
	t *testing.T,
	identity recoveryset.NativeBackupIdentity,
	inspection server.ClickHouseRecoveryDatabaseInspection,
	operationID uuid.UUID,
) {
	t.Helper()
	if identity.ServerVersion != inspection.ServerVersion ||
		identity.MigrationLedgerSHA256 != hex.EncodeToString(inspection.MigrationLedger.SHA256[:]) ||
		identity.DatabaseUUID != inspection.DatabaseUUID ||
		identity.SchemaMigrationsTableUUID != inspection.SchemaMigrationsTableUUID ||
		identity.EventsTableUUID != inspection.EventsTableUUID ||
		identity.RecoverySetsTableUUID != inspection.RecoverySetsTableUUID ||
		identity.RecoveryArchiveMarkersTableUUID != inspection.RecoveryArchiveMarkersTableUUID ||
		identity.MaxVisibilitySeq != inspection.MaximumVisibilitySequence ||
		identity.BackupOperationUUID != operationID.String() {
		t.Fatalf("native backup identity = %#v, inspection = %#v", identity, inspection)
	}
}

type deploymentRecoveryTestSession struct {
	clickhousedriver.Conn
	ping              func(context.Context) error
	queryRow          func(context.Context, string, ...any) clickhouserow.Row
	selectRows        func(context.Context, any, string, ...any) error
	exec              func(context.Context, string, ...any) error
	archiveMarkers    map[string]deploymentRecoveryTestArchiveMarker
	markerOperation   func(string, string)
	markerTruncateErr error
	closeCalls        int
}

type deploymentRecoveryTestArchiveMarker struct {
	recoverySetID string
	operationUUID string
}

func allowDeploymentRecoveryReadOnlyDisk(
	context.Context,
	server.ClickHouseRecoveryDiskConnection,
) error {
	return nil
}

func (session *deploymentRecoveryTestSession) Ping(ctx context.Context) error {
	if session.ping == nil {
		return nil
	}
	return session.ping(ctx)
}

func (session *deploymentRecoveryTestSession) QueryRow(
	ctx context.Context,
	query string,
	args ...any,
) clickhouserow.Row {
	if databaseName, ok := deploymentRecoveryTestArchiveMarkerReadDatabase(query); ok {
		if session.markerOperation != nil {
			session.markerOperation("read", databaseName)
		}
		marker, exists := session.archiveMarkers[databaseName]
		if !exists {
			return deploymentRecoveryTestRow{values: []any{uint64(0), uint8(0), "", ""}}
		}
		return deploymentRecoveryTestRow{values: []any{
			uint64(1), uint8(1), marker.recoverySetID, marker.operationUUID,
		}}
	}
	if session.queryRow == nil {
		return deploymentRecoveryTestRow{err: fmt.Errorf("unexpected recovery query %q", query)}
	}
	return session.queryRow(ctx, query, args...)
}

func (session *deploymentRecoveryTestSession) Select(
	ctx context.Context,
	destination any,
	query string,
	args ...any,
) error {
	if session.selectRows == nil {
		return fmt.Errorf("unexpected recovery select %q", query)
	}
	return session.selectRows(ctx, destination, query, args...)
}

func (session *deploymentRecoveryTestSession) Exec(
	ctx context.Context,
	query string,
	args ...any,
) error {
	if databaseName, ok := deploymentRecoveryTestArchiveMarkerInsertDatabase(query); ok {
		if len(args) != 3 {
			return fmt.Errorf("archive marker INSERT arguments = %#v", args)
		}
		slot, slotOK := args[0].(uint8)
		recoverySetID, recoverySetIDOK := args[1].(string)
		operationUUID, operationUUIDOK := args[2].(string)
		if !slotOK || slot != 1 || !recoverySetIDOK || !operationUUIDOK {
			return fmt.Errorf("archive marker INSERT arguments = %#v", args)
		}
		if session.archiveMarkers == nil {
			session.archiveMarkers = make(map[string]deploymentRecoveryTestArchiveMarker)
		}
		session.archiveMarkers[databaseName] = deploymentRecoveryTestArchiveMarker{
			recoverySetID: recoverySetID,
			operationUUID: operationUUID,
		}
		if session.markerOperation != nil {
			session.markerOperation("insert", databaseName)
		}
		return nil
	}
	if databaseName, ok := deploymentRecoveryTestArchiveMarkerTruncateDatabase(query); ok {
		if len(args) != 0 {
			return fmt.Errorf("archive marker TRUNCATE arguments = %#v", args)
		}
		if session.markerTruncateErr != nil {
			return session.markerTruncateErr
		}
		delete(session.archiveMarkers, databaseName)
		if session.markerOperation != nil {
			session.markerOperation("truncate", databaseName)
		}
		return nil
	}
	if session.exec == nil {
		return fmt.Errorf("unexpected recovery exec %q", query)
	}
	return session.exec(ctx, query, args...)
}

func (session *deploymentRecoveryTestSession) Close() error {
	session.closeCalls++
	return nil
}

type deploymentRecoveryTestRow struct {
	values []any
	err    error
}

func (row deploymentRecoveryTestRow) Err() error { return row.err }

func (row deploymentRecoveryTestRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return fmt.Errorf("recovery row destinations = %d, want %d", len(destinations), len(row.values))
	}
	for index, value := range row.values {
		switch destination := destinations[index].(type) {
		case *string:
			converted, ok := value.(string)
			if !ok {
				return fmt.Errorf("recovery row value %d is %T, want string", index, value)
			}
			*destination = converted
		case *uint64:
			converted, ok := value.(uint64)
			if !ok {
				return fmt.Errorf("recovery row value %d is %T, want uint64", index, value)
			}
			*destination = converted
		case *uint8:
			converted, ok := value.(uint8)
			if !ok {
				return fmt.Errorf("recovery row value %d is %T, want uint8", index, value)
			}
			*destination = converted
		default:
			return fmt.Errorf("unsupported recovery row destination %T", destinations[index])
		}
	}
	return nil
}

func (deploymentRecoveryTestRow) ScanStruct(any) error {
	return errors.New("deployment recovery test row does not support ScanStruct")
}

func deploymentRecoveryTestArchiveMarkerReadDatabase(query string) (string, bool) {
	const tableSuffix = ".recovery_archive_markers"
	trimmed := strings.TrimSpace(query)
	tableEnd := strings.Index(trimmed, tableSuffix)
	if tableEnd < 0 {
		return "", false
	}
	fromIndex := strings.LastIndex(trimmed[:tableEnd], "FROM ")
	if fromIndex < 0 {
		return "", false
	}
	databaseName := strings.TrimSpace(trimmed[fromIndex+len("FROM ") : tableEnd])
	if databaseName == "" || strings.ContainsAny(databaseName, " \t\r\n") {
		return "", false
	}
	return databaseName, true
}

func deploymentRecoveryTestArchiveMarkerInsertDatabase(query string) (string, bool) {
	const (
		prefix = "INSERT INTO "
		suffix = ".recovery_archive_markers "
	)
	if !strings.HasPrefix(query, prefix) {
		return "", false
	}
	remaining := strings.TrimPrefix(query, prefix)
	before, _, ok := strings.Cut(remaining, suffix)
	if !ok {
		return "", false
	}
	return before, true
}

func deploymentRecoveryTestArchiveMarkerTruncateDatabase(query string) (string, bool) {
	const (
		prefix = "TRUNCATE TABLE "
		suffix = ".recovery_archive_markers SYNC"
	)
	if !strings.HasPrefix(query, prefix) || !strings.HasSuffix(query, suffix) {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(query, prefix), suffix), true
}

func TestDeploymentRecoveryDatabaseNamespaceUsesBoundedSortedPrefixQuery(t *testing.T) {
	t.Parallel()

	wantNames := []string{
		deploymentRecoveryDatabase,
	}
	session := &deploymentRecoveryTestSession{
		selectRows: func(_ context.Context, destination any, query string, args ...any) error {
			if query != "SELECT name FROM system.databases WHERE startsWith(name, ?) ORDER BY name LIMIT 2" ||
				len(args) != 1 || args[0] != deploymentRecoveryDatabase {
				t.Fatalf("database namespace query = %q args=%#v", query, args)
			}
			rows, ok := destination.(*[]deploymentRecoveryDatabaseNameRow)
			if !ok {
				t.Fatalf("database namespace destination = %T", destination)
			}
			*rows = make([]deploymentRecoveryDatabaseNameRow, len(wantNames))
			for index, name := range wantNames {
				(*rows)[index] = deploymentRecoveryDatabaseNameRow{Name: name}
			}
			return nil
		},
	}

	names, err := deploymentRecoveryDatabaseNamespace(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("database namespace = %v, want %v", names, wantNames)
	}
}

func TestClassifyDeploymentRecoveryDatabaseNamespace(t *testing.T) {
	t.Parallel()

	recoverySetID := strings.Repeat("a", 32)
	for _, test := range []struct {
		name      string
		databases []string
		wantFinal bool
		wantError bool
	}{
		{name: "empty"},
		{name: "exact final", databases: []string{deploymentRecoveryDatabase}, wantFinal: true},
		{name: "legacy fixed staging", databases: []string{"open_splunk_restore"}, wantError: true},
		{name: "legacy suffixed staging", databases: []string{"open_splunk_restore_" + strings.Repeat("b", 32)}, wantError: true},
		{name: "archive alias", databases: []string{deploymentRecoveryArchiveDatabasePrefix + recoverySetID}, wantError: true},
		{name: "unexpected reserved name", databases: []string{"open_splunk_other"}, wantError: true},
		{name: "outside prefix", databases: []string{"other"}, wantError: true},
		{name: "final and foreign", databases: []string{deploymentRecoveryDatabase, "open_splunk_other"}, wantError: true},
		{name: "unsorted", databases: []string{"open_splunk_other", deploymentRecoveryDatabase}, wantError: true},
		{name: "duplicate", databases: []string{deploymentRecoveryDatabase, deploymentRecoveryDatabase}, wantError: true},
		{name: "over bound", databases: []string{deploymentRecoveryDatabase, "open_splunk_other", "open_splunk_restore"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			finalExists, err := classifyDeploymentRecoveryDatabaseNamespace(test.databases)
			if (err != nil) != test.wantError {
				t.Fatalf("classification error = %v, wantError=%t", err, test.wantError)
			}
			if finalExists != test.wantFinal {
				t.Fatalf("classification = %t, want %t", finalExists, test.wantFinal)
			}
		})
	}
}

func TestDeploymentRecoveryRestoreRejectsForeignNamespaceBeforeMutation(t *testing.T) {
	t.Parallel()

	verification := deploymentRecoveryVerificationFixture(testRecoveryReleaseIdentity())
	for _, test := range []struct {
		name      string
		databases []string
	}{
		{
			name:      "legacy fixed staging",
			databases: []string{"open_splunk_restore"},
		},
		{
			name:      "legacy suffixed staging",
			databases: []string{"open_splunk_restore_" + strings.Repeat("b", 32)},
		},
		{
			name:      "archive alias",
			databases: []string{deploymentRecoveryArchiveDatabasePrefix + verification.Manifest.RecoverySetID},
		},
		{
			name:      "unexpected reserved database",
			databases: []string{"open_splunk_orphan"},
		},
		{
			name:      "final and foreign reserved database",
			databases: []string{deploymentRecoveryDatabase, "open_splunk_other"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mutated := false
			markMutation := func() {
				mutated = true
			}
			err := runDeploymentRestoreStateMachine(
				t.Context(),
				&deploymentRecoveryTestSession{},
				verification,
				func(context.Context) (recoveryset.Verification, error) {
					markMutation()
					return recoveryset.Verification{}, nil
				},
				deploymentRecoveryDependencies{
					migrationFiles: migrations.ClickHouse(),
					listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
						return test.databases, nil
					},
					validateRestoreDisk: func(context.Context, server.ClickHouseRecoveryDiskConnection) error {
						markMutation()
						return nil
					},
					inspectDatabase: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS, string) (server.ClickHouseRecoveryDatabaseInspection, error) {
						markMutation()
						return server.ClickHouseRecoveryDatabaseInspection{}, nil
					},
					readReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, string, string) (server.ClickHouseRecoveryReceipt, error) {
						markMutation()
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
						markMutation()
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					newOperationUUID: func() (uuid.UUID, error) {
						markMutation()
						return uuid.Nil, nil
					},
					now: func() time.Time {
						markMutation()
						return time.Time{}
					},
				},
			)
			if err == nil || !strings.Contains(err.Error(), "database namespace") {
				t.Fatalf("foreign namespace restore error = %v", err)
			}
			if mutated {
				t.Fatal("foreign database namespace reached a ClickHouse restore mutation")
			}
		})
	}
}

func TestDeploymentRecoveryBackupRejectsNativeStatusAndSourceDrift(t *testing.T) {
	for _, test := range []struct {
		name              string
		operation         string
		status            string
		nativeErr         error
		cancelNative      bool
		wantMarkerRetain  bool
		markerTruncateErr error
		replacePostMarker bool
		mutatePost        func(*server.ClickHouseRecoveryDatabaseInspection)
	}{
		{name: "wrong operation ID", operation: "66666666-6666-4666-8666-666666666666", status: "BACKUP_CREATED", wantMarkerRetain: true},
		{name: "nonterminal status", operation: "55555555-5555-4555-8555-555555555555", status: "CREATING_BACKUP", wantMarkerRetain: true},
		{name: "transport error after submission", nativeErr: errors.New("read native backup result"), wantMarkerRetain: true},
		{name: "canceled before terminal result", nativeErr: context.Canceled, cancelNative: true, wantMarkerRetain: true},
		{name: "canceled after native completion", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", cancelNative: true},
		{name: "marker cleanup fails after terminal completion", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", wantMarkerRetain: true, markerTruncateErr: errors.New("truncate exact source marker")},
		{name: "source marker changes after terminal completion", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", wantMarkerRetain: true, replacePostMarker: true},
		{name: "visibility drift", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", mutatePost: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.MaximumVisibilitySequence++
		}},
		{name: "ledger drift", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", mutatePost: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.MigrationLedger.SHA256[0]++
		}},
		{name: "UUID drift", operation: "55555555-5555-4555-8555-555555555555", status: "BACKUP_CREATED", mutatePost: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.EventsTableUUID = "77777777-7777-4777-8777-777777777777"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRecoveryCommandFixture(t)
			operationID := uuid.MustParse("55555555-5555-4555-8555-555555555555")
			recoverySetID := strings.Repeat("a", 32)
			inspection := deploymentRecoveryInspectionFixture()
			validations := 0
			backupContext, cancelBackup := context.WithCancel(t.Context())
			defer cancelBackup()
			session := &deploymentRecoveryTestSession{
				markerTruncateErr: test.markerTruncateErr,
				queryRow: func(context.Context, string, ...any) clickhouserow.Row {
					if test.cancelNative {
						cancelBackup()
					}
					if test.nativeErr != nil {
						return deploymentRecoveryTestRow{err: test.nativeErr}
					}
					return deploymentRecoveryTestRow{values: []any{test.operation, test.status}}
				},
			}
			markerReads := 0
			session.markerOperation = func(operation, databaseName string) {
				if operation != "read" || !test.replacePostMarker {
					return
				}
				markerReads++
				if markerReads == 3 {
					session.archiveMarkers[databaseName] = deploymentRecoveryTestArchiveMarker{
						recoverySetID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						operationUUID: "77777777-7777-4777-8777-777777777777",
					}
				}
			}
			completedCreate := false
			err := runBackupDeploymentRecoverySetWithDependencies(
				backupContext,
				fixture.backupOptions(),
				fixture.release,
				deploymentRecoveryDependencies{
					migrationFiles:           migrations.ClickHouse(),
					acquireDatabaseLock:      func(string) (*serverLock, error) { return &serverLock{}, nil },
					open:                     func(*clickhousedriver.Options) (deploymentRecoverySession, error) { return session, nil },
					validateBackupPrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error { return nil },
					validateBackupSource: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS) (server.ClickHouseRecoveryDatabaseInspection, error) {
						validations++
						value := inspection
						if validations == 2 && test.mutatePost != nil {
							test.mutatePost(&value)
						}
						return value, nil
					},
					createRecoverySet: func(ctx context.Context, options recoveryset.CreateOptions) (recoveryset.Verification, error) {
						_, createErr := options.NativeBackup(ctx, recoveryset.NativeBackupRequest{
							RecoverySetID:   recoverySetID,
							Disk:            deploymentRecoveryDisk,
							Database:        deploymentRecoveryDatabase,
							ArchiveDatabase: deploymentRecoveryArchiveDatabasePrefix + recoverySetID,
							ArchiveName:     recoverySetID + deploymentRecoveryArchiveSuffix,
							ArchivePath:     filepath.Join(fixture.archiveRoot, recoverySetID+deploymentRecoveryArchiveSuffix),
						})
						if createErr == nil {
							completedCreate = true
						}
						return recoveryset.Verification{}, createErr
					},
					newOperationUUID: func() (uuid.UUID, error) { return operationID, nil },
				},
			)
			if err == nil {
				t.Fatal("backup accepted non-exact native completion or changing source")
			}
			if completedCreate {
				t.Fatal("backup published after native status/source failure")
			}
			if session.closeCalls != 1 {
				t.Fatalf("backup failure close calls = %d, want 1", session.closeCalls)
			}
			marker, markerRetained := session.archiveMarkers[deploymentRecoveryDatabase]
			if test.wantMarkerRetain {
				wantMarker := deploymentRecoveryTestArchiveMarker{
					recoverySetID: recoverySetID,
					operationUUID: operationID.String(),
				}
				if test.replacePostMarker {
					wantMarker = deploymentRecoveryTestArchiveMarker{
						recoverySetID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						operationUUID: "77777777-7777-4777-8777-777777777777",
					}
				}
				if !markerRetained || marker != wantMarker {
					t.Fatalf("ambiguous backup marker = (%#v, %t), want retained %#v", marker, markerRetained, wantMarker)
				}
				for _, identity := range []string{recoverySetID, operationID.String(), "reconciliation"} {
					if !strings.Contains(err.Error(), identity) {
						t.Fatalf("ambiguous backup error = %v, want identity %q", err, identity)
					}
				}
			} else if markerRetained || len(session.archiveMarkers) != 0 {
				t.Fatalf("post-terminal backup failure retained source archive marker: %#v", session.archiveMarkers)
			}
			wantValidations := 1
			if test.mutatePost != nil || test.markerTruncateErr != nil {
				wantValidations = 2
			}
			if validations != wantValidations {
				t.Fatalf("backup source validations = %d, want %d", validations, wantValidations)
			}
		})
	}
}

func TestDeploymentRecoveryBackupPreservesPreexistingDifferentArchiveMarker(t *testing.T) {
	fixture := newDeploymentRecoveryCommandFixture(t)
	const recoverySetID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	operationID := uuid.MustParse("55555555-5555-4555-8555-555555555555")
	stale := deploymentRecoveryTestArchiveMarker{
		recoverySetID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		operationUUID: "77777777-7777-4777-8777-777777777777",
	}
	var markerOperations []string
	session := &deploymentRecoveryTestSession{
		archiveMarkers: map[string]deploymentRecoveryTestArchiveMarker{
			deploymentRecoveryDatabase: stale,
		},
		markerOperation: func(operation, databaseName string) {
			markerOperations = append(markerOperations, operation+"-"+databaseName)
		},
		queryRow: func(_ context.Context, query string, _ ...any) clickhouserow.Row {
			t.Fatalf("stale source marker allowed native backup query %q", query)
			return deploymentRecoveryTestRow{}
		},
	}
	validations := 0
	err := runBackupDeploymentRecoverySetWithDependencies(
		t.Context(),
		fixture.backupOptions(),
		fixture.release,
		deploymentRecoveryDependencies{
			migrationFiles:      migrations.ClickHouse(),
			acquireDatabaseLock: func(string) (*serverLock, error) { return &serverLock{}, nil },
			open:                func(*clickhousedriver.Options) (deploymentRecoverySession, error) { return session, nil },
			validateBackupPrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
				return nil
			},
			validateBackupSource: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS) (server.ClickHouseRecoveryDatabaseInspection, error) {
				validations++
				return deploymentRecoveryInspectionFixture(), nil
			},
			createRecoverySet: func(ctx context.Context, options recoveryset.CreateOptions) (recoveryset.Verification, error) {
				_, createErr := options.NativeBackup(ctx, recoveryset.NativeBackupRequest{
					RecoverySetID:   recoverySetID,
					Disk:            deploymentRecoveryDisk,
					Database:        deploymentRecoveryDatabase,
					ArchiveDatabase: deploymentRecoveryArchiveDatabasePrefix + recoverySetID,
					ArchiveName:     recoverySetID + deploymentRecoveryArchiveSuffix,
					ArchivePath: filepath.Join(
						fixture.archiveRoot,
						recoverySetID+deploymentRecoveryArchiveSuffix,
					),
				})
				return recoveryset.Verification{}, createErr
			},
			newOperationUUID: func() (uuid.UUID, error) { return operationID, nil },
		},
	)
	if !errors.Is(err, server.ErrClickHouseRecoveryArchiveMarkerMismatch) {
		t.Fatalf("stale source marker error = %v, want marker mismatch", err)
	}
	for _, diagnostic := range []string{recoverySetID, operationID.String(), "reconciliation"} {
		if !strings.Contains(err.Error(), diagnostic) {
			t.Fatalf("stale source marker error = %v, want reconciliation identity %q", err, diagnostic)
		}
	}
	if got := session.archiveMarkers[deploymentRecoveryDatabase]; got != stale {
		t.Fatalf("stale source marker changed = %#v, want %#v", got, stale)
	}
	if got, want := markerOperations, []string{"read-open_splunk", "read-open_splunk"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stale source marker operations = %#v, want %#v", got, want)
	}
	if validations != 1 || session.closeCalls != 1 {
		t.Fatalf("stale marker validations/closes = (%d, %d), want (1, 1)", validations, session.closeCalls)
	}
}

func TestDeploymentRecoveryRestoreFreshStateReceiptsCanonicalThenRestoresControlPlane(t *testing.T) {
	fixture := newDeploymentRecoveryCommandFixture(t)
	verification := deploymentRecoveryVerificationFixture(fixture.release)
	recoverySetID := verification.Manifest.RecoverySetID
	databaseName := deploymentRecoveryDatabase
	operationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	restoredAt := time.Date(2026, time.August, 2, 12, 34, 56, 789_000_000, time.UTC)
	inspection := deploymentRestoredInspectionFixture(databaseName)
	var order []string
	nativeRestored := false
	session := &deploymentRecoveryTestSession{
		archiveMarkers: map[string]deploymentRecoveryTestArchiveMarker{
			databaseName: {
				recoverySetID: recoverySetID,
				operationUUID: verification.Manifest.ClickHouse.BackupOperationUUID,
			},
		},
		markerOperation: func(operation, databaseName string) {
			order = append(order, "marker-"+operation+"-"+databaseName)
		},
		ping: func(context.Context) error {
			order = append(order, "ping")
			return nil
		},
		queryRow: func(_ context.Context, query string, args ...any) clickhouserow.Row {
			order = append(order, "native-restore")
			wantQuery := "RESTORE DATABASE open_splunk_recovery_" + recoverySetID +
				" AS " + databaseName +
				" FROM Disk('open_splunk_recovery', '" + recoverySetID +
				".tar.zst') SETTINGS id = '" + operationID.String() +
				"', create_database = 'create', create_table = 'create', allow_non_empty_tables = false"
			if query != wantQuery || len(args) != 0 {
				t.Fatalf("native restore query = %q args=%#v, want %q without arguments", query, args, wantQuery)
			}
			nativeRestored = true
			return deploymentRecoveryTestRow{values: []any{operationID.String(), "RESTORED"}}
		},
		exec: func(_ context.Context, query string, args ...any) error {
			t.Fatalf("direct canonical restore issued unexpected SQL mutation %q args=%#v", query, args)
			return nil
		},
	}
	err := runRestoreDeploymentRecoverySetWithDependencies(
		t.Context(),
		fixture.restoreOptions(),
		fixture.release,
		deploymentRecoveryDependencies{
			migrationFiles: migrations.ClickHouse(),
			acquireDatabaseLock: func(path string) (*serverLock, error) {
				order = append(order, "lock")
				if path != fixture.databasePath {
					t.Fatalf("restore lock path = %q, want %q", path, fixture.databasePath)
				}
				return acquireDeploymentRecoveryTestServerLock(t, path)
			},
			verifyRecoverySet: func(_ context.Context, options recoveryset.VerifyOptions) (recoveryset.Verification, error) {
				order = append(order, "verify")
				if options.Source != fixture.destination || options.ArchiveRoot != fixture.archiveRoot ||
					options.Release != fixture.release || options.ArchiveOwnership != deploymentRecoveryArchiveOwnership() {
					t.Fatalf("restore verification options = %#v", options)
				}
				return verification, nil
			},
			preflightControlPlane: func(_ context.Context, options controlbackup.RestoreOptions) error {
				order = append(order, "preflight")
				if options.DatabaseLock == nil ||
					options.DatabaseLock.Name() != fixture.databasePath+".server.lock" ||
					options.ExpectedRecoverySetID != verification.Manifest.RecoverySetID ||
					options.ExpectedManifestSHA256 != verification.Manifest.ControlPlane.Manifest.SHA256 {
					t.Fatalf("control-plane preflight lock = %#v", options.DatabaseLock)
				}
				return nil
			},
			open: func(options *clickhousedriver.Options) (deploymentRecoverySession, error) {
				order = append(order, "open")
				assertDeploymentRecoveryConnectionOptions(t, options, "open_splunk_restore", fixture.password)
				return session, nil
			},
			validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
				order = append(order, "privileges")
				return nil
			},
			validateRestoreDisk: func(context.Context, server.ClickHouseRecoveryDiskConnection) error {
				order = append(order, "disk")
				return nil
			},
			listRecoveryDatabases: func(_ context.Context, got deploymentRecoverySession) ([]string, error) {
				if got != session {
					t.Fatal("database namespace inspection received different session")
				}
				order = append(order, "namespace")
				if nativeRestored {
					return []string{deploymentRecoveryDatabase}, nil
				}
				return nil, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				order = append(order, "uuid")
				return operationID, nil
			},
			inspectDatabase: func(_ context.Context, got server.ClickHouseRecoveryValidationConnection, gotFS fs.FS, databaseName string) (server.ClickHouseRecoveryDatabaseInspection, error) {
				order = append(order, "inspect-"+databaseName)
				if got != session || gotFS == nil || databaseName != deploymentRecoveryDatabase {
					t.Fatal("restore inspection received different dependencies")
				}
				value := inspection
				return value, nil
			},
			readReceipt: func(_ context.Context, got server.ClickHouseRecoveryReceiptConnection, databaseName, gotID, manifestSHA string) (server.ClickHouseRecoveryReceipt, error) {
				order = append(order, "read-receipt-"+databaseName)
				if got != session || databaseName != deploymentRecoveryDatabase ||
					gotID != recoverySetID || manifestSHA != verification.ManifestSHA256 {
					t.Fatalf("restore read receipt inputs = (%T, %q, %q, %q)", got, databaseName, gotID, manifestSHA)
				}
				return server.ClickHouseRecoveryReceipt{
					RecoverySetID:                   gotID,
					DeploymentManifestSHA256:        manifestSHA,
					DatabaseUUID:                    inspection.DatabaseUUID,
					SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
					EventsTableUUID:                 inspection.EventsTableUUID,
					RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
					RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
					RestoredAt:                      restoredAt,
				}, nil
			},
			writeReceipt: func(_ context.Context, got server.ClickHouseRecoveryReceiptConnection, databaseName string, receipt server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
				order = append(order, "receipt")
				if got != session || databaseName != deploymentRecoveryDatabase ||
					receipt.RecoverySetID != recoverySetID ||
					receipt.DeploymentManifestSHA256 != verification.ManifestSHA256 ||
					receipt.DatabaseUUID != inspection.DatabaseUUID ||
					receipt.SchemaMigrationsTableUUID != inspection.SchemaMigrationsTableUUID ||
					receipt.EventsTableUUID != inspection.EventsTableUUID ||
					receipt.RecoverySetsTableUUID != inspection.RecoverySetsTableUUID ||
					receipt.RecoveryArchiveMarkersTableUUID != inspection.RecoveryArchiveMarkersTableUUID ||
					!receipt.RestoredAt.Equal(restoredAt) {
					t.Fatalf("restore receipt inputs = (%T, %q, %#v)", got, databaseName, receipt)
				}
				return receipt, nil
			},
			restoreControlPlane: func(_ context.Context, options controlbackup.RestoreOptions) error {
				order = append(order, "control")
				if options.Source != filepath.Join(fixture.destination, "control-plane") ||
					options.DatabasePath != fixture.databasePath ||
					options.DatabaseLock == nil ||
					options.DatabaseLock.Name() != fixture.databasePath+".server.lock" ||
					options.MasterKeyPath != fixture.masterKeyPath ||
					options.AdministratorTokenPath != fixture.administratorTokenPath ||
					options.Release != fixture.release ||
					options.ExpectedRecoverySetID != verification.Manifest.RecoverySetID ||
					options.ExpectedManifestSHA256 != verification.Manifest.ControlPlane.Manifest.SHA256 {
					t.Fatalf("control-plane restore options = %#v", options)
				}
				return nil
			},
			now: func() time.Time {
				order = append(order, "time")
				return restoredAt
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"lock", "verify", "preflight", "open", "ping", "privileges", "namespace",
		"disk", "uuid", "native-restore", "namespace", "verify", "inspect-" + databaseName,
		"marker-read-" + databaseName, "time", "receipt",
		"inspect-" + databaseName, "read-receipt-" + databaseName,
		"marker-read-" + databaseName, "marker-truncate-" + databaseName,
		"marker-read-" + databaseName,
		"inspect-" + databaseName, "read-receipt-" + databaseName,
		"marker-read-" + databaseName, "namespace", "control",
	}
	if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("fresh restore order = %v, want %v", order, wantOrder)
	}
	if session.closeCalls != 1 {
		t.Fatalf("fresh restore session close calls = %d, want 1", session.closeCalls)
	}
}

func TestDeploymentPostRestoreVerificationRequiresExactManifestAndDigest(t *testing.T) {
	t.Parallel()

	expected := deploymentRecoveryVerificationFixture(testRecoveryReleaseIdentity())
	if err := validateDeploymentPostRestoreVerification(expected, expected); err != nil {
		t.Fatalf("exact post-restore verification failed: %v", err)
	}
	changedManifest := expected
	changedManifest.Manifest.ClickHouse.Archive.SHA256 = strings.Repeat("e", 64)
	if err := validateDeploymentPostRestoreVerification(expected, changedManifest); err == nil {
		t.Fatal("changed post-restore archive digest succeeded")
	}
	changedDigest := expected
	changedDigest.ManifestSHA256 = strings.Repeat("f", 64)
	if err := validateDeploymentPostRestoreVerification(expected, changedDigest); err == nil {
		t.Fatal("changed post-restore manifest digest succeeded")
	}
}

func TestDeploymentRecoveryRestoreHoldsRealDatabaseLockThroughRealControlRestore(t *testing.T) {
	fixture := newDeploymentRecoveryCommandFixture(t)
	root := filepath.Dir(fixture.databasePath)
	restoreDirectory := filepath.Join(root, "restored-control")
	if err := os.Mkdir(restoreDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.databasePath = filepath.Join(restoreDirectory, "control.db")
	fixture.masterKeyPath = filepath.Join(restoreDirectory, "master.key")
	fixture.administratorTokenPath = filepath.Join(restoreDirectory, "administrator.token")
	controlManifest, controlManifestSHA256 := createDeploymentControlBackupFixture(t, &fixture)

	verification := bindDeploymentVerificationToControlBackup(
		deploymentRecoveryVerificationFixture(fixture.release),
		controlManifest,
		controlManifestSHA256,
	)
	inspection := deploymentRestoredInspectionFixture(deploymentRecoveryDatabase)
	session := &deploymentRecoveryTestSession{}
	hostLockPath := filepath.Join(root, "deployment-recovery-host.lock")
	var acquiredLock *serverLock
	controlRestored := false
	err := runRestoreDeploymentRecoverySetWithDependencies(
		t.Context(),
		fixture.restoreOptions(),
		fixture.release,
		deploymentRecoveryDependencies{
			migrationFiles: migrations.ClickHouse(),
			acquireDatabaseLock: func(path string) (*serverLock, error) {
				var lockErr error
				acquiredLock, lockErr = acquireServerLockAt(path, hostLockPath)
				return acquiredLock, lockErr
			},
			verifyRecoverySet: func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error) {
				return verification, nil
			},
			preflightControlPlane: controlbackup.PreflightRestore,
			open: func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
				return session, nil
			},
			validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
				return nil
			},
			validateRestoreDisk: allowDeploymentRecoveryReadOnlyDisk,
			listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
				return []string{deploymentRecoveryDatabase}, nil
			},
			inspectDatabase: func(_ context.Context, _ server.ClickHouseRecoveryValidationConnection, _ fs.FS, databaseName string) (server.ClickHouseRecoveryDatabaseInspection, error) {
				if databaseName != deploymentRecoveryDatabase {
					t.Fatalf("inspected recovery database = %q", databaseName)
				}
				return inspection, nil
			},
			readReceipt: func(_ context.Context, _ server.ClickHouseRecoveryReceiptConnection, databaseName, recoverySetID, manifestSHA string) (server.ClickHouseRecoveryReceipt, error) {
				if databaseName != deploymentRecoveryDatabase ||
					recoverySetID != verification.Manifest.RecoverySetID ||
					manifestSHA != verification.ManifestSHA256 {
					t.Fatalf("receipt identity = (%q, %q, %q)", databaseName, recoverySetID, manifestSHA)
				}
				return server.ClickHouseRecoveryReceipt{
					RecoverySetID:                   recoverySetID,
					DeploymentManifestSHA256:        manifestSHA,
					DatabaseUUID:                    inspection.DatabaseUUID,
					SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
					EventsTableUUID:                 inspection.EventsTableUUID,
					RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
					RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
					RestoredAt:                      time.Date(2026, 8, 2, 12, 34, 56, 0, time.UTC),
				}, nil
			},
			writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
				t.Fatal("resumed native restore rewrote its receipt")
				return server.ClickHouseRecoveryReceipt{}, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				t.Fatal("resumed native restore allocated an operation UUID")
				return uuid.Nil, nil
			},
			now: func() time.Time {
				t.Fatal("resumed native restore allocated a receipt time")
				return time.Time{}
			},
			restoreControlPlane: func(ctx context.Context, options controlbackup.RestoreOptions) error {
				if options.DatabaseLock == nil ||
					options.DatabaseLock.Name() != options.DatabasePath+".server.lock" {
					t.Fatalf("control restore database lock = %#v", options.DatabaseLock)
				}
				if acquiredLock == nil || len(acquiredLock.files) != 2 ||
					acquiredLock.files[1] != options.DatabaseLock {
					t.Fatalf("database lock is not held through control restore: %#v", acquiredLock)
				}
				controlRestored = true
				return controlbackup.Restore(ctx, options)
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !controlRestored || acquiredLock == nil || len(acquiredLock.files) != 0 {
		t.Fatalf("control restore/lock lifecycle = (%t, %#v)", controlRestored, acquiredLock)
	}
	lockInfo, err := os.Lstat(fixture.databasePath + ".server.lock")
	if err != nil {
		t.Fatal(err)
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 || lockInfo.Size() != 0 {
		t.Fatalf("preserved database lock state = (%v, %#o, %d)", lockInfo.Mode(), lockInfo.Mode().Perm(), lockInfo.Size())
	}
	restored, err := control.OpenReadOnly(t.Context(), fixture.databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := restored.VerifyCurrentMigrations(t.Context(), migrations.SQLite()); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentRecoveryRestorePreflightRejectsUnsafeControlStateBeforeEveryClickHouseCallback(t *testing.T) {
	for _, test := range []struct {
		name      string
		seed      func(*testing.T, *deploymentRecoveryCommandFixture, controlbackup.Manifest)
		afterLock func(*testing.T, *deploymentRecoveryCommandFixture)
	}{
		{
			name: "unsafe held lock mode",
			seed: func(t *testing.T, fixture *deploymentRecoveryCommandFixture, _ controlbackup.Manifest) {
				lockPath := fixture.databasePath + ".server.lock"
				if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(lockPath, 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "held lock pathname replacement",
			afterLock: func(t *testing.T, fixture *deploymentRecoveryCommandFixture) {
				lockPath := fixture.databasePath + ".server.lock"
				movedPath := filepath.Join(filepath.Dir(fixture.destination), "moved-deployment-lock")
				if err := os.Rename(lockPath, movedPath); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unrelated destination entry",
			seed: func(t *testing.T, fixture *deploymentRecoveryCommandFixture, _ controlbackup.Manifest) {
				if err := os.WriteFile(
					filepath.Join(filepath.Dir(fixture.databasePath), "unrelated"),
					[]byte("x"),
					0o600,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "inconsistent publication prefix",
			seed: func(t *testing.T, fixture *deploymentRecoveryCommandFixture, manifest controlbackup.Manifest) {
				contents, err := os.ReadFile(filepath.Join(
					fixture.destination,
					"control-plane",
					manifest.MasterKey.Name,
				))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fixture.masterKeyPath, contents, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unsafe source member",
			seed: func(t *testing.T, fixture *deploymentRecoveryCommandFixture, manifest controlbackup.Manifest) {
				if err := os.Chmod(filepath.Join(
					fixture.destination,
					"control-plane",
					manifest.MasterKey.Name,
				), 0o640); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRecoveryCommandFixture(t)
			root := filepath.Dir(fixture.databasePath)
			restoreDirectory := filepath.Join(root, "restored-control")
			if err := os.Mkdir(restoreDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			fixture.databasePath = filepath.Join(restoreDirectory, "control.db")
			fixture.masterKeyPath = filepath.Join(restoreDirectory, "master.key")
			fixture.administratorTokenPath = filepath.Join(restoreDirectory, "administrator.token")
			controlManifest, controlManifestSHA256 := createDeploymentControlBackupFixture(t, &fixture)
			if test.seed != nil {
				test.seed(t, &fixture, controlManifest)
			}

			verification := bindDeploymentVerificationToControlBackup(
				deploymentRecoveryVerificationFixture(fixture.release),
				controlManifest,
				controlManifestSHA256,
			)
			clickHouseTouched := false
			touchClickHouse := func() { clickHouseTouched = true }
			err := runRestoreDeploymentRecoverySetWithDependencies(
				t.Context(),
				fixture.restoreOptions(),
				fixture.release,
				deploymentRecoveryDependencies{
					migrationFiles: migrations.ClickHouse(),
					acquireDatabaseLock: func(path string) (*serverLock, error) {
						return acquireServerLockAt(
							path,
							filepath.Join(root, "adversarial-host.server.lock"),
						)
					},
					verifyRecoverySet: func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error) {
						if test.afterLock != nil {
							test.afterLock(t, &fixture)
						}
						return verification, nil
					},
					preflightControlPlane: controlbackup.PreflightRestore,
					open: func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
						touchClickHouse()
						return nil, errors.New("ClickHouse must not be opened")
					},
					validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error {
						touchClickHouse()
						return nil
					},
					validateRestoreDisk: func(context.Context, server.ClickHouseRecoveryDiskConnection) error {
						touchClickHouse()
						return nil
					},
					listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
						touchClickHouse()
						return nil, nil
					},
					inspectDatabase: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS, string) (server.ClickHouseRecoveryDatabaseInspection, error) {
						touchClickHouse()
						return server.ClickHouseRecoveryDatabaseInspection{}, nil
					},
					readReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, string, string) (server.ClickHouseRecoveryReceipt, error) {
						touchClickHouse()
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
						touchClickHouse()
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					newOperationUUID: func() (uuid.UUID, error) {
						touchClickHouse()
						return uuid.Nil, nil
					},
					now: func() time.Time {
						touchClickHouse()
						return time.Time{}
					},
					restoreControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						touchClickHouse()
						return nil
					},
				},
			)
			if err == nil {
				t.Fatal("deployment restore accepted unsafe control-plane preflight state")
			}
			if clickHouseTouched {
				t.Fatal("unsafe control-plane preflight state reached a ClickHouse callback or later mutation")
			}
		})
	}
}

func TestDeploymentRecoveryRestoreResumesExactCanonicalReceiptWithMarkerCrashStates(t *testing.T) {
	for _, test := range []struct {
		name        string
		markerExact bool
	}{
		{name: "receipt with exact retained marker", markerExact: true},
		{name: "receipt with already consumed marker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRecoveryCommandFixture(t)
			verification := deploymentRecoveryVerificationFixture(fixture.release)
			markerState := make(map[string]deploymentRecoveryTestArchiveMarker)
			if test.markerExact {
				markerState[deploymentRecoveryDatabase] = deploymentRecoveryTestArchiveMarker{
					recoverySetID: verification.Manifest.RecoverySetID,
					operationUUID: verification.Manifest.ClickHouse.BackupOperationUUID,
				}
			}
			var order []string
			session := &deploymentRecoveryTestSession{
				archiveMarkers: markerState,
				markerOperation: func(operation, databaseName string) {
					order = append(order, "marker-"+operation+"-"+databaseName)
				},
				exec: func(_ context.Context, query string, _ ...any) error {
					t.Fatalf("resumed canonical restore issued unexpected mutation %q", query)
					return nil
				},
				queryRow: func(_ context.Context, query string, _ ...any) clickhouserow.Row {
					t.Fatalf("resumed restore issued native operation %q", query)
					return deploymentRecoveryTestRow{}
				},
			}
			err := runRestoreDeploymentRecoverySetWithDependencies(
				t.Context(),
				fixture.restoreOptions(),
				fixture.release,
				deploymentRecoveryDependencies{
					migrationFiles: migrations.ClickHouse(),
					acquireDatabaseLock: func(path string) (*serverLock, error) {
						return acquireDeploymentRecoveryTestServerLock(t, path)
					},
					verifyRecoverySet: func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error) {
						order = append(order, "verify")
						return verification, nil
					},
					preflightControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						order = append(order, "preflight")
						return nil
					},
					open:                      func(*clickhousedriver.Options) (deploymentRecoverySession, error) { return session, nil },
					validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error { return nil },
					validateRestoreDisk:       allowDeploymentRecoveryReadOnlyDisk,
					listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
						order = append(order, "namespace")
						return []string{deploymentRecoveryDatabase}, nil
					},
					inspectDatabase: func(_ context.Context, _ server.ClickHouseRecoveryValidationConnection, _ fs.FS, databaseName string) (server.ClickHouseRecoveryDatabaseInspection, error) {
						order = append(order, "inspect-"+databaseName)
						inspection := deploymentRestoredInspectionFixture(databaseName)
						return inspection, nil
					},
					readReceipt: func(_ context.Context, _ server.ClickHouseRecoveryReceiptConnection, databaseName, recoverySetID, manifestSHA string) (server.ClickHouseRecoveryReceipt, error) {
						order = append(order, "receipt-"+databaseName)
						if databaseName != deploymentRecoveryDatabase || recoverySetID != verification.Manifest.RecoverySetID ||
							manifestSHA != verification.ManifestSHA256 {
							t.Fatalf("resume receipt inputs = (%q, %q, %q)", databaseName, recoverySetID, manifestSHA)
						}
						inspection := deploymentRestoredInspectionFixture(databaseName)
						return server.ClickHouseRecoveryReceipt{
							RecoverySetID:                   recoverySetID,
							DeploymentManifestSHA256:        manifestSHA,
							DatabaseUUID:                    inspection.DatabaseUUID,
							SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
							EventsTableUUID:                 inspection.EventsTableUUID,
							RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
							RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
							RestoredAt:                      time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC),
						}, nil
					},
					writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
						t.Fatal("resumed restore rewrote receipt")
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					newOperationUUID: func() (uuid.UUID, error) {
						t.Fatal("resumed restore allocated a native operation UUID")
						return uuid.Nil, nil
					},
					now: func() time.Time {
						t.Fatal("resumed restore allocated a new receipt time")
						return time.Time{}
					},
					restoreControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						order = append(order, "control")
						return nil
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			wantOrder := []string{
				"verify", "preflight", "namespace", "inspect-open_splunk", "receipt-open_splunk",
			}
			wantOrder = append(wantOrder, "marker-read-open_splunk")
			if test.markerExact {
				wantOrder = append(wantOrder, "marker-truncate-open_splunk", "marker-read-open_splunk")
			}
			wantOrder = append(wantOrder, "namespace", "control")
			if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
				t.Fatalf("resume order = %v, want %v", order, wantOrder)
			}
		})
	}
}

func TestDeploymentRecoveryRestoreFailsClosedForPartialOrMismatchedState(t *testing.T) {
	for _, test := range []struct {
		name             string
		mutateInspection func(*server.ClickHouseRecoveryDatabaseInspection)
		receiptErr       error
		wrongMarker      bool
	}{
		{name: "canonical missing receipt", receiptErr: server.ErrClickHouseRecoveryReceiptMismatch},
		{name: "canonical UUID differs from receipt", mutateInspection: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.DatabaseUUID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
		}},
		{name: "canonical visibility mismatch", mutateInspection: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.MaximumVisibilitySequence++
		}},
		{name: "canonical migration ledger mismatch", mutateInspection: func(value *server.ClickHouseRecoveryDatabaseInspection) {
			value.MigrationLedger.SHA256[0]++
		}},
		{name: "canonical marker belongs to another backup", wrongMarker: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRecoveryCommandFixture(t)
			verification := deploymentRecoveryVerificationFixture(fixture.release)
			marker := deploymentRecoveryTestArchiveMarker{
				recoverySetID: verification.Manifest.RecoverySetID,
				operationUUID: verification.Manifest.ClickHouse.BackupOperationUUID,
			}
			if test.wrongMarker {
				marker.recoverySetID = strings.Repeat("f", 32)
			}
			session := &deploymentRecoveryTestSession{
				archiveMarkers: map[string]deploymentRecoveryTestArchiveMarker{
					deploymentRecoveryDatabase: marker,
				},
				queryRow: func(_ context.Context, query string, _ ...any) clickhouserow.Row {
					t.Fatalf("partial-state restore issued native operation %q", query)
					return deploymentRecoveryTestRow{}
				},
				exec: func(_ context.Context, query string, _ ...any) error {
					t.Fatalf("partial-state restore issued unexpected mutation %q", query)
					return nil
				},
			}
			controlRestored := false
			err := runRestoreDeploymentRecoverySetWithDependencies(
				t.Context(),
				fixture.restoreOptions(),
				fixture.release,
				deploymentRecoveryDependencies{
					migrationFiles: migrations.ClickHouse(),
					acquireDatabaseLock: func(path string) (*serverLock, error) {
						return acquireDeploymentRecoveryTestServerLock(t, path)
					},
					verifyRecoverySet: func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error) {
						return verification, nil
					},
					preflightControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						return nil
					},
					open:                      func(*clickhousedriver.Options) (deploymentRecoverySession, error) { return session, nil },
					validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error { return nil },
					validateRestoreDisk:       allowDeploymentRecoveryReadOnlyDisk,
					listRecoveryDatabases: func(context.Context, deploymentRecoverySession) ([]string, error) {
						return []string{deploymentRecoveryDatabase}, nil
					},
					inspectDatabase: func(_ context.Context, _ server.ClickHouseRecoveryValidationConnection, _ fs.FS, databaseName string) (server.ClickHouseRecoveryDatabaseInspection, error) {
						value := deploymentRestoredInspectionFixture(databaseName)
						if test.mutateInspection != nil {
							test.mutateInspection(&value)
						}
						return value, nil
					},
					readReceipt: func(_ context.Context, _ server.ClickHouseRecoveryReceiptConnection, databaseName, recoverySetID, manifestSHA string) (server.ClickHouseRecoveryReceipt, error) {
						if test.receiptErr != nil {
							return server.ClickHouseRecoveryReceipt{}, test.receiptErr
						}
						inspection := deploymentRestoredInspectionFixture(databaseName)
						return server.ClickHouseRecoveryReceipt{
							RecoverySetID:                   recoverySetID,
							DeploymentManifestSHA256:        manifestSHA,
							DatabaseUUID:                    inspection.DatabaseUUID,
							SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
							EventsTableUUID:                 inspection.EventsTableUUID,
							RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
							RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
							RestoredAt:                      time.Now(),
						}, nil
					},
					writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
						t.Fatal("partial-state restore wrote receipt")
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					newOperationUUID: func() (uuid.UUID, error) {
						t.Fatal("partial-state restore allocated native operation")
						return uuid.Nil, nil
					},
					now: time.Now,
					restoreControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						controlRestored = true
						return nil
					},
				},
			)
			if err == nil {
				t.Fatal("restore accepted partial or mismatched ClickHouse state")
			}
			if controlRestored {
				t.Fatal("restore mutated control plane after rejecting ClickHouse state")
			}
			if got := session.archiveMarkers[deploymentRecoveryDatabase]; got != marker {
				t.Fatalf("failed restore mutated retained archive marker: %#v, want %#v", got, marker)
			}
			if session.closeCalls != 1 {
				t.Fatalf("partial-state session closes = %d, want 1", session.closeCalls)
			}
		})
	}
}

func TestDeploymentRecoveryRestoreRejectsNativeStatusBeforeReceiptOrControl(t *testing.T) {
	for _, test := range []struct {
		name       string
		returnedID string
		status     string
	}{
		{name: "wrong operation ID", returnedID: "77777777-7777-4777-8777-777777777777", status: "RESTORED"},
		{name: "nonterminal", returnedID: "66666666-6666-4666-8666-666666666666", status: "RESTORING"},
		{name: "failed", returnedID: "66666666-6666-4666-8666-666666666666", status: "RESTORE_FAILED"},
		{name: "empty", returnedID: "66666666-6666-4666-8666-666666666666", status: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDeploymentRecoveryCommandFixture(t)
			verification := deploymentRecoveryVerificationFixture(fixture.release)
			operationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
			session := &deploymentRecoveryTestSession{
				queryRow: func(context.Context, string, ...any) clickhouserow.Row {
					return deploymentRecoveryTestRow{values: []any{test.returnedID, test.status}}
				},
				exec: func(_ context.Context, query string, _ ...any) error {
					t.Fatalf("failed native restore executed post-restore mutation %q", query)
					return nil
				},
			}
			mutated := false
			err := runRestoreDeploymentRecoverySetWithDependencies(
				t.Context(), fixture.restoreOptions(), fixture.release,
				deploymentRecoveryDependencies{
					migrationFiles: migrations.ClickHouse(),
					acquireDatabaseLock: func(path string) (*serverLock, error) {
						return acquireDeploymentRecoveryTestServerLock(t, path)
					},
					verifyRecoverySet: func(context.Context, recoveryset.VerifyOptions) (recoveryset.Verification, error) {
						return verification, nil
					},
					preflightControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						return nil
					},
					open:                      func(*clickhousedriver.Options) (deploymentRecoverySession, error) { return session, nil },
					validateRestorePrivileges: func(context.Context, server.ClickHousePrivilegeConnection) error { return nil },
					validateRestoreDisk:       allowDeploymentRecoveryReadOnlyDisk,
					listRecoveryDatabases:     func(context.Context, deploymentRecoverySession) ([]string, error) { return nil, nil },
					inspectDatabase: func(context.Context, server.ClickHouseRecoveryValidationConnection, fs.FS, string) (server.ClickHouseRecoveryDatabaseInspection, error) {
						mutated = true
						return server.ClickHouseRecoveryDatabaseInspection{}, nil
					},
					readReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, string, string) (server.ClickHouseRecoveryReceipt, error) {
						mutated = true
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					writeReceipt: func(context.Context, server.ClickHouseRecoveryReceiptConnection, string, server.ClickHouseRecoveryReceipt) (server.ClickHouseRecoveryReceipt, error) {
						mutated = true
						return server.ClickHouseRecoveryReceipt{}, nil
					},
					newOperationUUID: func() (uuid.UUID, error) { return operationID, nil },
					now:              time.Now,
					restoreControlPlane: func(context.Context, controlbackup.RestoreOptions) error {
						mutated = true
						return nil
					},
				},
			)
			if err == nil {
				t.Fatalf("restore accepted native result (%q, %q)", test.returnedID, test.status)
			}
			if mutated {
				t.Fatalf("restore result (%q, %q) allowed post-native mutation", test.returnedID, test.status)
			}
			if session.closeCalls != 1 {
				t.Fatalf("restore status failure closes = %d, want 1", session.closeCalls)
			}
		})
	}
}

func TestOpenDeploymentRecoverySessionRejectsUnsafeTLSAndPasswordBeforeNetwork(t *testing.T) {
	identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	invalidCA := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(invalidCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		passwordFile string
		caFile       string
	}{
		{name: "group-writable password", passwordFile: writeClickHouseCredentialFixture(t, "secret", 0o660), caFile: identity.CertificateFile},
		{name: "invalid CA", passwordFile: writeClickHouseCredentialFixture(t, "secret", 0o600), caFile: invalidCA},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened := false
			_, err := openDeploymentRecoverySession(
				t.Context(), "clickhouse:9440", test.passwordFile, test.caFile, "clickhouse",
				"open_splunk_backup",
				func(*clickhousedriver.Options) (deploymentRecoverySession, error) {
					opened = true
					return &deploymentRecoveryTestSession{}, nil
				},
			)
			if err == nil {
				t.Fatal("recovery session accepted unsafe TLS or password file")
			}
			if opened {
				t.Fatal("recovery session opened network before rejecting local identity")
			}
		})
	}
}

func deploymentRecoveryVerificationFixture(
	release controlbackup.ReleaseIdentity,
) recoveryset.Verification {
	inspection := deploymentRecoveryInspectionFixture()
	recoverySetID := strings.Repeat("a", 32)
	return recoveryset.Verification{
		ManifestSHA256: strings.Repeat("b", 64),
		Manifest: recoveryset.Manifest{
			FormatVersion:        1,
			CreatedAtUnixMicro:   time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC).UnixMicro(),
			RecoverySetID:        recoverySetID,
			Scope:                "deployment-recovery-set",
			ClickHouseIncluded:   true,
			ApplicationVersion:   release.ApplicationVersion,
			SourceRevision:       release.SourceRevision,
			SQLiteMigrations:     release.SQLiteMigrations,
			ClickHouseMigrations: release.ClickHouseMigrations,
			ControlPlane: recoveryset.ControlPlaneIdentity{
				Directory: "control-plane",
				Manifest: recoveryset.FileIdentity{
					Name: "control-plane/manifest.json", SizeBytes: 1, SHA256: strings.Repeat("c", 64),
				},
			},
			ClickHouse: recoveryset.ClickHouseIdentity{
				ServerVersion:   inspection.ServerVersion,
				Disk:            "open_splunk_recovery",
				Database:        "open_splunk",
				ArchiveDatabase: "open_splunk_recovery_" + recoverySetID,
				ArchiveFormat:   "clickhouse-native-tar-zstd",
				Archive: recoveryset.FileIdentity{
					Name: recoverySetID + ".tar.zst", SizeBytes: 1, SHA256: strings.Repeat("d", 64),
				},
				MigrationLedgerSHA256:           hex.EncodeToString(inspection.MigrationLedger.SHA256[:]),
				DatabaseUUID:                    inspection.DatabaseUUID,
				SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
				EventsTableUUID:                 inspection.EventsTableUUID,
				RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
				RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
				MaxVisibilitySeq:                inspection.MaximumVisibilitySequence,
				BackupOperationUUID:             "55555555-5555-4555-8555-555555555555",
			},
		},
	}
}
