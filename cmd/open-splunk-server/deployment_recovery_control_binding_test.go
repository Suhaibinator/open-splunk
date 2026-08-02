package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
)

func TestDeploymentRecoveryExactFinalRetryRejectsSameReleaseControlChildSwap(t *testing.T) {
	swapFixture := newDeploymentRecoveryControlChildSwapFixture(t)
	fixture := swapFixture.command
	verification := swapFixture.verification
	inspection := deploymentRestoredInspectionFixture(deploymentRecoveryDatabase)
	session := &deploymentRecoveryTestSession{}
	verificationCalls := 0
	clickHouseFinalValidated := false
	controlRestoreCalled := false
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
				verificationCalls++
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
			inspectDatabase: func(
				_ context.Context,
				_ server.ClickHouseRecoveryValidationConnection,
				_ fs.FS,
				databaseName string,
			) (server.ClickHouseRecoveryDatabaseInspection, error) {
				if databaseName != deploymentRecoveryDatabase {
					t.Fatalf("exact-final retry inspected %q", databaseName)
				}
				clickHouseFinalValidated = true
				return inspection, nil
			},
			readReceipt: func(
				_ context.Context,
				_ server.ClickHouseRecoveryReceiptConnection,
				databaseName string,
				recoverySetID string,
				manifestSHA256 string,
			) (server.ClickHouseRecoveryReceipt, error) {
				if databaseName != deploymentRecoveryDatabase ||
					recoverySetID != verification.Manifest.RecoverySetID ||
					manifestSHA256 != verification.ManifestSHA256 {
					t.Fatalf(
						"exact-final receipt identity = (%q, %q, %q)",
						databaseName,
						recoverySetID,
						manifestSHA256,
					)
				}
				return server.ClickHouseRecoveryReceipt{
					RecoverySetID:                   recoverySetID,
					DeploymentManifestSHA256:        manifestSHA256,
					DatabaseUUID:                    inspection.DatabaseUUID,
					SchemaMigrationsTableUUID:       inspection.SchemaMigrationsTableUUID,
					EventsTableUUID:                 inspection.EventsTableUUID,
					RecoverySetsTableUUID:           inspection.RecoverySetsTableUUID,
					RecoveryArchiveMarkersTableUUID: inspection.RecoveryArchiveMarkersTableUUID,
					RestoredAt:                      time.Date(2026, 8, 2, 12, 34, 56, 0, time.UTC),
				}, nil
			},
			writeReceipt: func(
				context.Context,
				server.ClickHouseRecoveryReceiptConnection,
				string,
				server.ClickHouseRecoveryReceipt,
			) (server.ClickHouseRecoveryReceipt, error) {
				t.Fatal("exact-final retry rewrote its receipt")
				return server.ClickHouseRecoveryReceipt{}, nil
			},
			newOperationUUID: func() (uuid.UUID, error) {
				t.Fatal("exact-final retry allocated an operation UUID")
				return uuid.Nil, nil
			},
			now: func() time.Time {
				t.Fatal("exact-final retry allocated a receipt time")
				return time.Time{}
			},
			restoreControlPlane: func(ctx context.Context, options controlbackup.RestoreOptions) error {
				controlRestoreCalled = true
				return swapFixture.swapAndRestore(ctx, options)
			},
		},
	)
	if err == nil || !errors.Is(err, controlbackup.ErrDeploymentBindingMismatch) {
		t.Fatalf("same-release control-child swap error = %v", err)
	}
	if verificationCalls != 1 || !clickHouseFinalValidated || !controlRestoreCalled {
		t.Fatalf(
			"exact-final swap lifecycle = verify %d ClickHouse %t control %t",
			verificationCalls,
			clickHouseFinalValidated,
			controlRestoreCalled,
		)
	}
	swapFixture.requireTargetsAbsent(t)
}

func TestDeploymentRecoveryFreshCanonicalRestoreRejectsSameReleaseControlChildSwap(t *testing.T) {
	swapFixture := newDeploymentRecoveryControlChildSwapFixture(t)
	fixture := swapFixture.command
	verification := swapFixture.verification
	recoverySetID := verification.Manifest.RecoverySetID
	databaseName := deploymentRecoveryDatabase
	operationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	restoredAt := time.Date(2026, 8, 2, 12, 34, 56, 789_000_000, time.UTC)
	nativeRestoreCalled := false
	session := &deploymentRecoveryTestSession{
		archiveMarkers: make(map[string]deploymentRecoveryTestArchiveMarker),
	}
	session.queryRow = func(_ context.Context, query string, args ...any) clickhouserow.Row {
		wantQuery := deploymentRecoveryRestoreQuery(
			verification,
			databaseName,
			operationID,
		)
		if query != wantQuery || len(args) != 0 {
			t.Fatalf("fresh native restore query = %q args=%#v, want %q", query, args, wantQuery)
		}
		nativeRestoreCalled = true
		session.archiveMarkers[databaseName] = deploymentRecoveryTestArchiveMarker{
			recoverySetID: recoverySetID,
			operationUUID: verification.Manifest.ClickHouse.BackupOperationUUID,
		}
		return deploymentRecoveryTestRow{
			values: []any{operationID.String(), deploymentRecoveryRestoreCompleteStatus},
		}
	}
	session.exec = func(_ context.Context, query string, args ...any) error {
		t.Fatalf("fresh direct restore issued unexpected mutation %q args=%#v", query, args)
		return nil
	}
	verificationCalls := 0
	inspectionCalls := 0
	controlRestoreCalled := false
	var receipt server.ClickHouseRecoveryReceipt
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
				verificationCalls++
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
				if nativeRestoreCalled {
					return []string{deploymentRecoveryDatabase}, nil
				}
				return nil, nil
			},
			inspectDatabase: func(
				_ context.Context,
				_ server.ClickHouseRecoveryValidationConnection,
				_ fs.FS,
				databaseName string,
			) (server.ClickHouseRecoveryDatabaseInspection, error) {
				if databaseName != deploymentRecoveryDatabase {
					t.Fatalf("fresh restore inspected unexpected database %q", databaseName)
				}
				inspectionCalls++
				return deploymentRestoredInspectionFixture(databaseName), nil
			},
			readReceipt: func(
				_ context.Context,
				_ server.ClickHouseRecoveryReceiptConnection,
				databaseName string,
				gotRecoverySetID string,
				manifestSHA256 string,
			) (server.ClickHouseRecoveryReceipt, error) {
				if databaseName != deploymentRecoveryDatabase ||
					gotRecoverySetID != recoverySetID ||
					manifestSHA256 != verification.ManifestSHA256 ||
					receipt.RecoverySetID == "" {
					t.Fatalf(
						"fresh receipt read = (%q, %q, %q, %#v)",
						databaseName,
						gotRecoverySetID,
						manifestSHA256,
						receipt,
					)
				}
				return receipt, nil
			},
			writeReceipt: func(
				_ context.Context,
				_ server.ClickHouseRecoveryReceiptConnection,
				databaseName string,
				value server.ClickHouseRecoveryReceipt,
			) (server.ClickHouseRecoveryReceipt, error) {
				if databaseName != deploymentRecoveryDatabase || value.RecoverySetID != recoverySetID ||
					value.DeploymentManifestSHA256 != verification.ManifestSHA256 ||
					!value.RestoredAt.Equal(restoredAt) {
					t.Fatalf("fresh receipt write = (%q, %#v)", databaseName, value)
				}
				receipt = value
				return value, nil
			},
			newOperationUUID: func() (uuid.UUID, error) { return operationID, nil },
			now:              func() time.Time { return restoredAt },
			restoreControlPlane: func(ctx context.Context, options controlbackup.RestoreOptions) error {
				controlRestoreCalled = true
				return swapFixture.swapAndRestore(ctx, options)
			},
		},
	)
	if err == nil || !errors.Is(err, controlbackup.ErrDeploymentBindingMismatch) {
		t.Fatalf("fresh same-release control-child swap error = %v", err)
	}
	if verificationCalls != 2 || inspectionCalls != 3 || !nativeRestoreCalled ||
		!controlRestoreCalled || receipt.RecoverySetID == "" ||
		len(session.archiveMarkers) != 0 || session.closeCalls != 1 {
		t.Fatalf(
			"fresh swap lifecycle = verify %d inspect %d native %t control %t receipt=%#v markers=%v closes=%d",
			verificationCalls,
			inspectionCalls,
			nativeRestoreCalled,
			controlRestoreCalled,
			receipt,
			session.archiveMarkers,
			session.closeCalls,
		)
	}
	swapFixture.requireTargetsAbsent(t)
}

type deploymentRecoveryControlChildSwapFixture struct {
	command         deploymentRecoveryCommandFixture
	verification    recoveryset.Verification
	replacementPath string
}

func newDeploymentRecoveryControlChildSwapFixture(
	t *testing.T,
) deploymentRecoveryControlChildSwapFixture {
	t.Helper()
	fixture := newDeploymentRecoveryCommandFixture(t)
	root := filepath.Dir(fixture.databasePath)
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	restoreDirectory := filepath.Join(root, "restored-control")
	if err := os.Mkdir(restoreDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture.databasePath = filepath.Join(restoreDirectory, "control.db")
	fixture.masterKeyPath = filepath.Join(restoreDirectory, "master.key")
	fixture.administratorTokenPath = filepath.Join(restoreDirectory, "administrator.token")
	originalManifest, originalManifestSHA256 := createDeploymentControlBackupFixture(t, &fixture)

	sourceDirectory := filepath.Join(root, "source-control")
	replacementPath := filepath.Join(root, "replacement-control-plane")
	replacementManifest, err := controlbackup.Create(t.Context(), controlbackup.CreateOptions{
		DatabasePath:           filepath.Join(sourceDirectory, "control.db"),
		MasterKeyPath:          filepath.Join(sourceDirectory, "master.key"),
		AdministratorTokenPath: filepath.Join(sourceDirectory, "administrator.token"),
		Destination:            replacementPath,
		Release:                fixture.release,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replacementManifest.RecoverySetID == originalManifest.RecoverySetID ||
		replacementManifest.ReleaseIdentity() != originalManifest.ReleaseIdentity() {
		t.Fatalf(
			"replacement/original identities = (%q, %#v) / (%q, %#v)",
			replacementManifest.RecoverySetID,
			replacementManifest.ReleaseIdentity(),
			originalManifest.RecoverySetID,
			originalManifest.ReleaseIdentity(),
		)
	}
	return deploymentRecoveryControlChildSwapFixture{
		command: fixture,
		verification: bindDeploymentVerificationToControlBackup(
			deploymentRecoveryVerificationFixture(fixture.release),
			originalManifest,
			originalManifestSHA256,
		),
		replacementPath: replacementPath,
	}
}

func (fixture deploymentRecoveryControlChildSwapFixture) swapAndRestore(
	ctx context.Context,
	options controlbackup.RestoreOptions,
) error {
	originalPath := filepath.Join(fixture.command.destination, "control-plane")
	preservedPath := filepath.Join(fixture.command.destination, "original-control-plane")
	if err := os.Rename(originalPath, preservedPath); err != nil {
		return err
	}
	if err := os.Rename(fixture.replacementPath, originalPath); err != nil {
		return err
	}
	return controlbackup.Restore(ctx, options)
}

func (fixture deploymentRecoveryControlChildSwapFixture) requireTargetsAbsent(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		fixture.command.databasePath,
		fixture.command.masterKeyPath,
		fixture.command.administratorTokenPath,
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("same-release control-child swap published %q: %v", path, err)
		}
	}
}
