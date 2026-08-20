package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/recoveryset"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/google/uuid"
)

const nativeRecoveryIntegrationArchiveRoot = "/var/lib/open-splunk-clickhouse-backups"

// TestDeploymentNativeRecoveryClickHouseLifecycle is opt-in because it owns a
// digest-pinned Docker container and named volume. It covers the production
// native BACKUP/RESTORE state machine rather than reimplementing its SQL in the
// test: exact synchronous status rows, alias-bound archives, direct restore into
// an absent canonical database, fresh restore UUIDs, marker-bound receipt
// publication and consumption, exact receipt retries, manifest validation, and
// the two one-shot principals' complete grant surfaces.
func TestDeploymentNativeRecoveryClickHouseLifecycle(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startNativeRecoveryIntegrationClickHouse(t, ctx, image)
	admin := fixture.open(t, fixture.bootstrapUsername, fixture.bootstrapPassword)
	t.Cleanup(func() {
		if closeErr := admin.Close(); closeErr != nil {
			t.Errorf("close recovery integration bootstrap session: %v", closeErr)
		}
	})

	if err := server.ApplyClickHouseMigrations(ctx, admin, migrations.ClickHouse()); err != nil {
		t.Fatalf("apply recovery integration schema: %v", err)
	}
	nativeRecoveryIntegrationRequireMigrationLedgerBounds(t, ctx, admin)
	nativeRecoveryIntegrationRequireSingletonReadBounds(t, ctx, admin)
	nativeRecoveryIntegrationRequireCatalogIsolation(t, ctx, admin)
	recoverySource := ingest.NativeCollectorSource("native-recovery-fixture-collector")
	if err := admin.Exec(ctx, `
		INSERT INTO open_splunk.events
			(event_id, event_time, index_time, raw, field_metadata_version,
			 collector_id, ingest_source_kind, ingest_source_id,
			 visibility_seq, expires_at)
		VALUES
			('recovery-fixture', now64(9), now64(3), 'durable recovery fixture', 1,
			 ?, ?, ?, 42, addYears(now64(3), 1))`,
		recoverySource.CollectorID,
		uint8(recoverySource.Kind),
		recoverySource.ID,
	); err != nil {
		t.Fatalf("seed recovery integration source: %v", err)
	}
	fixture.provisionRecoveryPrincipals(t, ctx, admin)

	backup := fixture.open(
		t,

		deploymentRecoveryBackupUsername,
		fixture.backupPassword)

	restore := fixture.open(
		t,

		deploymentRecoveryRestoreUsername,
		fixture.restorePassword)

	t.Cleanup(func() {
		if closeErr := restore.Close(); closeErr != nil {
			t.Errorf("close recovery integration restore session: %v", closeErr)
		}
		if closeErr := backup.Close(); closeErr != nil {
			t.Errorf("close recovery integration backup session: %v", closeErr)
		}
	})
	if err := server.ValidateClickHouseBackupPrivileges(ctx, backup); err != nil {
		t.Fatalf("validate exact backup principal: %v", err)
	}
	if err := server.ValidateClickHouseRestorePrivileges(ctx, restore); err != nil {
		t.Fatalf("validate exact restore principal: %v", err)
	}
	restoreGrants := nativeRecoveryIntegrationGrants(t, ctx, restore)
	t.Logf("pinned restore SHOW GRANTS rows: %q", restoreGrants)
	for _, prohibited := range []string{"BACKUP", "DROP TABLE", "system.backups"} {
		if strings.Contains(strings.Join(restoreGrants, "\n"), prohibited) {
			t.Fatalf("restore principal unexpectedly grants %q: %q", prohibited, restoreGrants)
		}
	}
	nativeRecoveryIntegrationRequirePrivilegeDenied(
		t,
		"restore raw event read",
		func() error {
			var raw string
			return restore.QueryRow(
				ctx,
				"SELECT raw FROM open_splunk.events LIMIT 1",
			).Scan(&raw)
		},
	)
	for _, tableName := range []string{"events", "schema_migrations"} {
		nativeRecoveryIntegrationRequirePrivilegeDenied(
			t,
			"restore truncate "+tableName,
			func() error {
				return restore.Exec(
					ctx,
					"TRUNCATE TABLE open_splunk."+tableName+" SYNC",
				)
			},
		)
	}
	for name, statement := range map[string]string{
		"restore unexpected table create": "CREATE TABLE open_splunk.unexpected_restore_table (value UInt8) ENGINE = Memory",
		"restore event table drop":        "DROP TABLE open_splunk.events SYNC",
		"restore canonical database drop": "DROP DATABASE open_splunk SYNC",
	} {
		nativeRecoveryIntegrationRequirePrivilegeDenied(
			t,
			name,
			func() error { return restore.Exec(ctx, statement) },
		)
	}
	if err := admin.Exec(
		ctx,
		"CREATE TABLE open_splunk.administrator_extra (value UInt8) ENGINE = Memory",
	); err != nil {
		t.Fatalf("create administrator-owned extra source table: %v", err)
	}
	if _, err := server.ValidateClickHouseBackupSource(
		ctx,
		backup,
		migrations.ClickHouse(),
	); !errors.Is(err, server.ErrClickHousePhysicalSchemaDrift) {
		t.Fatalf(
			"backup source with administrator-owned extra table error = %v, want physical schema drift",
			err,
		)
	}
	if err := admin.Exec(
		ctx,
		"CREATE TABLE open_splunk.administrator_extra_two (value UInt8) ENGINE = Memory",
	); err != nil {
		t.Fatalf("create second administrator-owned extra source table: %v", err)
	}
	if _, err := server.ValidateClickHouseBackupSource(
		ctx,
		backup,
		migrations.ClickHouse(),
	); !errors.Is(err, server.ErrClickHousePhysicalSchemaDrift) {
		var exception *clickhousedriver.Exception
		if !errors.As(err, &exception) || exception.Code != 158 {
			t.Fatalf(
				"backup source beyond physical-schema sentinel error = %v, want drift or bounded-read overflow",
				err,
			)
		}
	}
	if err := admin.Exec(ctx, "DROP TABLE open_splunk.administrator_extra_two SYNC"); err != nil {
		t.Fatalf("remove second administrator-owned extra source table: %v", err)
	}
	if err := admin.Exec(ctx, "DROP TABLE open_splunk.administrator_extra SYNC"); err != nil {
		t.Fatalf("remove administrator-owned extra source table: %v", err)
	}

	operationIDs := []uuid.UUID{
		uuid.MustParse("10000000-0000-4000-8000-000000000001"),
		uuid.MustParse("10000000-0000-4000-8000-000000000002"),
		uuid.MustParse("10000000-0000-4000-8000-000000000003"),
		uuid.MustParse("10000000-0000-4000-8000-000000000004"),
		uuid.MustParse("10000000-0000-4000-8000-000000000005"),
	}
	operationIndex := 0
	dependencies := defaultDeploymentRecoveryDependencies()
	dependencies.newOperationUUID = func() (uuid.UUID, error) {
		if operationIndex >= len(operationIDs) {
			return uuid.Nil, errors.New("recovery integration exhausted operation UUIDs")
		}
		value := operationIDs[operationIndex]
		operationIndex++
		return value, nil
	}
	dependencies.now = func() time.Time {
		return time.Date(2026, time.August, 2, 12, 34, 56, 789_000_000, time.UTC)
	}

	const (
		recoverySetA              = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		recoverySetB              = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		manifestHash              = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		sameAliasReplacementName  = "same-alias-replacement.tar.zst"
		verifiedOriginalSpareName = "verified-original-a.tar.zst"
	)
	requestA := nativeRecoveryIntegrationBackupRequest(recoverySetA)
	identityA, err := runDeploymentNativeBackup(
		ctx,
		backup,
		migrations.ClickHouse(),
		requestA,
		dependencies,
	)
	if err != nil {
		t.Fatalf("create first native recovery archive: %v", err)
	}
	originalArchiveIdentity := fixture.archiveIdentity(t, ctx, requestA.ArchiveName)
	if err := admin.Exec(ctx, `
		INSERT INTO open_splunk.events
			(event_id, event_time, index_time, raw, field_metadata_version,
			 collector_id, ingest_source_kind, ingest_source_id,
			 visibility_seq, expires_at)
		VALUES
			('same-alias-replacement', now64(9), now64(3), 'archive replacement payload', 1,
			 ?, ?, ?, 42, addYears(now64(3), 1))`,
		recoverySource.CollectorID,
		uint8(recoverySource.Kind),
		recoverySource.ID,
	); err != nil {
		t.Fatalf("seed same-alias replacement payload: %v", err)
	}
	replacementOperationID := uuid.MustParse("10000000-0000-4000-8000-000000000009")
	if err := runDeploymentRecoveryOperation(
		ctx,
		backup,
		fmt.Sprintf(
			"BACKUP DATABASE open_splunk AS %s TO Disk('%s', '%s') SETTINGS id = '%s'",
			requestA.ArchiveDatabase,
			deploymentRecoveryDisk,
			sameAliasReplacementName,
			replacementOperationID.String(),
		),
		replacementOperationID,
		deploymentRecoveryBackupCompleteStatus,
	); err != nil {
		t.Fatalf("create same-alias replacement archive: %v", err)
	}
	fixture.requireArchiveMetadata(t, ctx, sameAliasReplacementName)
	requestB := nativeRecoveryIntegrationBackupRequest(recoverySetB)
	identityB, err := runDeploymentNativeBackup(
		ctx,
		backup,
		migrations.ClickHouse(),
		requestB,
		dependencies,
	)
	if err != nil {
		t.Fatalf("create second native recovery archive: %v", err)
	}
	if identityA.BackupOperationUUID != operationIDs[0].String() ||
		identityB.BackupOperationUUID != operationIDs[1].String() ||
		identityA.BackupOperationUUID == identityB.BackupOperationUUID {
		t.Fatalf("backup operation identities = %q, %q", identityA.BackupOperationUUID, identityB.BackupOperationUUID)
	}
	if identityA.MaxVisibilitySeq != 42 || identityB.MaxVisibilitySeq != 42 {
		t.Fatalf("backup visibility identities = %d, %d, want 42", identityA.MaxVisibilitySeq, identityB.MaxVisibilitySeq)
	}
	if identityA.DatabaseUUID == "" ||
		identityA.SchemaMigrationsTableUUID == "" ||
		identityA.EventsTableUUID == "" ||
		identityA.RecoverySetsTableUUID == "" ||
		identityA.RecoveryArchiveMarkersTableUUID == "" {
		t.Fatalf("backup source UUID identity is incomplete: %#v", identityA)
	}
	if err := server.RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		backup,
		deploymentRecoveryDatabase,
	); err != nil {
		t.Fatalf("native backup retained its source archive marker: %v", err)
	}
	fixture.requireArchiveMetadata(t, ctx, requestA.ArchiveName)
	fixture.requireArchiveMetadata(t, ctx, requestB.ArchiveName)
	fixture.requireOperationStatus(
		t,
		ctx,
		admin,
		operationIDs[0],
		deploymentRecoveryBackupCompleteStatus,
	)
	fixture.requireOperationStatus(
		t,
		ctx,
		admin,
		operationIDs[1],
		deploymentRecoveryBackupCompleteStatus,
	)
	fixture.requireOperationStatus(
		t,
		ctx,
		admin,
		replacementOperationID,
		deploymentRecoveryBackupCompleteStatus,
	)

	verificationA := nativeRecoveryIntegrationVerification(
		recoverySetA,
		manifestHash,
		identityA,
	)
	verificationA.Manifest.ClickHouse.Archive = originalArchiveIdentity
	postRestoreVerify := func(callbackContext context.Context) (recoveryset.Verification, error) {
		reverified := verificationA
		reverified.Manifest.ClickHouse.Archive = fixture.archiveIdentity(
			t,
			callbackContext,
			requestA.ArchiveName,
		)
		return reverified, nil
	}
	canonicalDatabase := deploymentRecoveryDatabase

	restartRecoveryServer := func(recoveryReadOnly bool) {
		t.Helper()
		for name, connection := range map[string]clickhousedriver.Conn{
			"bootstrap": admin,
			"backup":    backup,
			"restore":   restore,
		} {
			if closeErr := connection.Close(); closeErr != nil {
				t.Fatalf("close %s session before recovery mount transition: %v", name, closeErr)
			}
		}
		fixture.restartContainer(t, ctx, recoveryReadOnly)
		admin = fixture.open(t, fixture.bootstrapUsername, fixture.bootstrapPassword)
		backup = fixture.open(t, deploymentRecoveryBackupUsername, fixture.backupPassword)
		restore = fixture.open(t, deploymentRecoveryRestoreUsername, fixture.restorePassword)
	}
	if err := server.ValidateClickHouseRecoveryDiskReadOnly(ctx, restore); !errors.Is(err, server.ErrClickHouseRecoveryDiskWritable) {
		t.Fatalf("writable recovery disk validation error = %v, want writable-disk rejection", err)
	}
	// Prepare the older different-alias adversarial case while the normal backup
	// topology is still writable, then freeze the same volume for restore.
	fixture.swapArchives(t, ctx, requestA.ArchiveName, requestB.ArchiveName)
	restartRecoveryServer(true)
	if err := server.ValidateClickHouseRecoveryDiskReadOnly(ctx, restore); err != nil {
		t.Fatalf("validate pinned read-only recovery disk: %v", err)
	}

	// A live/nonempty final database without the exact restored identity and
	// recovery receipt must never be interpreted as a successful prior restore.
	beforeFinalFailure := operationIndex
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "exact resumable restore") {
		t.Fatalf("non-restored final database error = %v, want exact-state failure", err)
	}
	if operationIndex != beforeFinalFailure {
		t.Fatal("final-state validation unexpectedly allocated a native operation ID")
	}

	if err := admin.Exec(ctx, "DROP DATABASE open_splunk SYNC"); err != nil {
		t.Fatalf("remove source before fresh-volume restore: %v", err)
	}
	if err := admin.Exec(ctx, "CREATE DATABASE "+canonicalDatabase); err != nil {
		t.Fatalf("create adversarial partial canonical database: %v", err)
	}
	if err := admin.Exec(ctx, "CREATE TABLE "+canonicalDatabase+".partial (value UInt8) ENGINE = Memory"); err != nil {
		t.Fatalf("create adversarial partial canonical table: %v", err)
	}
	beforePartialFailure := operationIndex
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "fresh ClickHouse data volume") {
		t.Fatalf("partial canonical error = %v, want fresh-volume failure", err)
	}
	if operationIndex != beforePartialFailure {
		t.Fatal("partial canonical validation unexpectedly allocated a native operation ID")
	}
	if err := admin.Exec(ctx, "DROP DATABASE "+canonicalDatabase+" SYNC"); err != nil {
		t.Fatalf("remove test-owned partial canonical database: %v", err)
	}

	// Swapping two individually valid archive files cannot bypass the database
	// alias embedded by ClickHouse in each native archive.
	swappedArchiveErr := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	)
	if swappedArchiveErr == nil {
		t.Fatal("swapped archive unexpectedly restored under the wrong embedded alias")
	}
	if operationIndex != 3 {
		t.Fatalf("swapped archive allocated operation count = %d, want 3", operationIndex)
	}
	fixture.requireOperationFailure(t, ctx, admin, operationIDs[2], "RESTORE_FAILED")
	restartRecoveryServer(false)
	fixture.swapArchives(t, ctx, requestA.ArchiveName, requestB.ArchiveName)
	restartRecoveryServer(true)
	if err := server.ValidateClickHouseRecoveryDiskReadOnly(ctx, restore); err != nil {
		t.Fatalf("revalidate read-only recovery disk after archive repair: %v", err)
	}
	if exists := nativeRecoveryIntegrationDatabaseExists(t, ctx, admin, canonicalDatabase); exists {
		// ClickHouse restore is not transactional. A failed native attempt may
		// leave a partial canonical database, which production refuses to resume.
		if err := admin.Exec(ctx, "DROP DATABASE "+canonicalDatabase+" SYNC"); err != nil {
			t.Fatalf("remove failed swapped-archive canonical database: %v", err)
		}
	}

	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err != nil {
		t.Fatalf("restore exact archive directly into the canonical database: %v", err)
	}
	if operationIndex != 4 {
		t.Fatalf("allocated native operation IDs = %d, want two backup and two restore attempts", operationIndex)
	}
	fixture.requireOperationStatus(
		t,
		ctx,
		admin,
		operationIDs[3],
		deploymentRecoveryRestoreCompleteStatus,
	)
	if !nativeRecoveryIntegrationDatabaseExists(t, ctx, admin, deploymentRecoveryDatabase) {
		t.Fatal("successful direct restore did not create the canonical database")
	}

	inspection, err := server.InspectClickHouseRecoveryDatabase(
		ctx,
		restore,
		migrations.ClickHouse(),
		deploymentRecoveryDatabase,
	)
	if err != nil {
		t.Fatalf("inspect restored canonical database: %v", err)
	}
	receipt, err := server.ReadClickHouseRecoveryReceipt(
		ctx,
		restore,
		deploymentRecoveryDatabase,
		recoverySetA,
		manifestHash,
	)
	if err != nil {
		t.Fatalf("read canonical recovery receipt: %v", err)
	}
	if receipt.DatabaseUUID != inspection.DatabaseUUID ||
		receipt.SchemaMigrationsTableUUID != inspection.SchemaMigrationsTableUUID ||
		receipt.EventsTableUUID != inspection.EventsTableUUID ||
		receipt.RecoverySetsTableUUID != inspection.RecoverySetsTableUUID ||
		receipt.RecoveryArchiveMarkersTableUUID != inspection.RecoveryArchiveMarkersTableUUID {
		t.Fatalf("receipt UUID identity = %#v, inspection = %#v", receipt, inspection)
	}
	if receipt.DatabaseUUID == identityA.DatabaseUUID ||
		receipt.SchemaMigrationsTableUUID == identityA.SchemaMigrationsTableUUID ||
		receipt.EventsTableUUID == identityA.EventsTableUUID ||
		receipt.RecoverySetsTableUUID == identityA.RecoverySetsTableUUID ||
		receipt.RecoveryArchiveMarkersTableUUID == identityA.RecoveryArchiveMarkersTableUUID {
		t.Fatalf("RESTORE AS reused a source UUID: source=%#v receipt=%#v", identityA, receipt)
	}
	if inspection.MaximumVisibilitySequence != 42 ||
		hex.EncodeToString(inspection.MigrationLedger.SHA256[:]) != identityA.MigrationLedgerSHA256 {
		t.Fatalf("restored manifest identity = %#v, backup = %#v", inspection, identityA)
	}
	if err := server.RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		restore,
		deploymentRecoveryDatabase,
	); err != nil {
		t.Fatalf("restored canonical database retained its archive marker: %v", err)
	}

	// A final-state retry is a pure validation path: it performs no second
	// native restore and consumes no fresh operation identity.
	beforeRetry := operationIndex
	operationRowsBefore := fixture.operationCount(t, ctx, admin)
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err != nil {
		t.Fatalf("resume exact canonical restore: %v", err)
	}
	if operationIndex != beforeRetry || fixture.operationCount(t, ctx, admin) != operationRowsBefore {
		t.Fatal("exact final-state retry issued another native operation")
	}

	// Physical schema and manifest identities remain independently enforced
	// even with an exact durable receipt.
	if err := admin.Exec(
		ctx,
		"ALTER TABLE open_splunk.events ADD COLUMN recovery_schema_drift UInt8 DEFAULT 0",
	); err != nil {
		t.Fatalf("create adversarial restored schema drift: %v", err)
	}
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); !errors.Is(err, server.ErrClickHousePhysicalSchemaDrift) {
		t.Fatalf("restored schema drift error = %v, want physical schema drift", err)
	}
	if err := admin.Exec(
		ctx,
		"ALTER TABLE open_splunk.events DROP COLUMN recovery_schema_drift",
	); err != nil {
		t.Fatalf("remove adversarial restored schema drift: %v", err)
	}
	if err := admin.Exec(
		ctx,
		"CREATE TABLE open_splunk.administrator_extra (value UInt8) ENGINE = Memory",
	); err != nil {
		t.Fatalf("create administrator-owned extra restored table: %v", err)
	}
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); !errors.Is(err, server.ErrClickHousePhysicalSchemaDrift) {
		t.Fatalf(
			"restored administrator-owned extra table error = %v, want physical schema drift",
			err,
		)
	}
	if err := admin.Exec(ctx, "DROP TABLE open_splunk.administrator_extra SYNC"); err != nil {
		t.Fatalf("remove administrator-owned extra restored table: %v", err)
	}
	wrongLedger := verificationA
	wrongLedger.Manifest.ClickHouse.MigrationLedgerSHA256 = strings.Repeat("e", 64)
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		wrongLedger,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "differs from the recovery manifest") {
		t.Fatalf("ledger mismatch error = %v, want manifest mismatch", err)
	}
	wrongVisibility := verificationA
	wrongVisibility.Manifest.ClickHouse.MaxVisibilitySeq++
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		wrongVisibility,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "differs from the recovery manifest") {
		t.Fatalf("visibility mismatch error = %v, want manifest mismatch", err)
	}

	// A foreign reserved alias must fail before receipt or marker mutation even
	// when the canonical database is otherwise an exact completed restore.
	const foreignDatabase = "open_splunk_other"
	if err := admin.Exec(ctx, "CREATE DATABASE "+foreignDatabase); err != nil {
		t.Fatalf("create foreign reserved database for adversarial state: %v", err)
	}
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "unexpected reserved database") {
		t.Fatalf("foreign database state error = %v, want fail-closed namespace rejection", err)
	}
	if err := admin.Exec(ctx, "DROP DATABASE "+foreignDatabase+" SYNC"); err != nil {
		t.Fatalf("remove adversarial foreign database: %v", err)
	}

	// A corrupt canonical receipt fails closed. Repairing that receipt and
	// simulating a crash after receipt publication but before marker consumption
	// allows an exact retry without another RESTORE.
	badReceipt := receipt
	badReceipt.DatabaseUUID = "20000000-0000-4000-8000-000000000001"
	if _, err := server.WriteClickHouseRecoveryReceipt(
		ctx,
		restore,
		canonicalDatabase,
		badReceipt,
	); err != nil {
		t.Fatalf("write adversarial mismatched receipt: %v", err)
	}
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err == nil || !strings.Contains(err.Error(), "exact resumable restore") {
		t.Fatalf("mismatched receipt retry error = %v, want fail-closed canonical state", err)
	}
	if _, err := server.WriteClickHouseRecoveryReceipt(
		ctx,
		restore,
		canonicalDatabase,
		receipt,
	); err != nil {
		t.Fatalf("repair exact canonical receipt: %v", err)
	}
	if err := admin.Exec(
		ctx,
		"INSERT INTO "+canonicalDatabase+".recovery_archive_markers "+
			"(slot, recovery_set_id, backup_operation_uuid) VALUES (?, ?, ?)",
		uint8(1),
		recoverySetA,
		identityA.BackupOperationUUID,
	); err != nil {
		t.Fatalf("restore exact marker for receipted canonical retry: %v", err)
	}
	beforeReceiptResume := operationIndex
	if err := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		postRestoreVerify,
		dependencies,
	); err != nil {
		t.Fatalf("resume exact receipted canonical retry: %v", err)
	}
	if operationIndex != beforeReceiptResume {
		t.Fatal("receipted canonical retry issued another native restore")
	}
	finalInspection, err := server.InspectClickHouseRecoveryDatabase(
		ctx,
		restore,
		migrations.ClickHouse(),
		deploymentRecoveryDatabase,
	)
	if err != nil {
		t.Fatalf("inspect canonical database after receipt retry: %v", err)
	}
	if finalInspection.DatabaseUUID != inspection.DatabaseUUID ||
		finalInspection.SchemaMigrationsTableUUID != inspection.SchemaMigrationsTableUUID ||
		finalInspection.EventsTableUUID != inspection.EventsTableUUID ||
		finalInspection.RecoverySetsTableUUID != inspection.RecoverySetsTableUUID ||
		finalInspection.RecoveryArchiveMarkersTableUUID != inspection.RecoveryArchiveMarkersTableUUID {
		t.Fatalf("receipt retry changed restore UUID identity: before=%#v after=%#v", inspection, finalInspection)
	}
	if err := server.RequireClickHouseRecoveryArchiveMarkerAbsent(
		ctx,
		restore,
		deploymentRecoveryDatabase,
	); err != nil {
		t.Fatalf("receipted canonical retry retained its archive marker: %v", err)
	}

	// A second valid archive can carry the same embedded database alias while
	// containing different event bytes. Simulate the verifier and ClickHouse
	// consuming distinct read-only volumes: external verification still returns
	// exact archive A while ClickHouse restores same-name archive B. The embedded
	// operation marker must reject B before a receipt or control publication.
	restartRecoveryServer(false)
	archiveRoot := nativeRecoveryIntegrationArchiveRoot
	fixture.docker(
		t,
		ctx,
		"exec", fixture.containerName,
		"mv",
		filepath.Join(archiveRoot, requestA.ArchiveName),
		filepath.Join(archiveRoot, verifiedOriginalSpareName),
	)
	fixture.docker(
		t,
		ctx,
		"exec", fixture.containerName,
		"mv",
		filepath.Join(archiveRoot, sameAliasReplacementName),
		filepath.Join(archiveRoot, requestA.ArchiveName),
	)
	restartRecoveryServer(true)
	if err := server.ValidateClickHouseRecoveryDiskReadOnly(ctx, restore); err != nil {
		t.Fatalf("validate read-only disk for same-alias replacement: %v", err)
	}
	if err := admin.Exec(ctx, "DROP DATABASE open_splunk SYNC"); err != nil {
		t.Fatalf("remove final database before same-alias replacement restore: %v", err)
	}
	beforeReplacementRestore := operationIndex
	replacementErr := runDeploymentRestoreStateMachine(
		ctx,
		restore,
		verificationA,
		func(context.Context) (recoveryset.Verification, error) {
			// Simulate the helper verifying original archive A on a distinct
			// read-only volume while ClickHouse consumed same-name archive B.
			return verificationA, nil
		},
		dependencies,
	)
	if replacementErr == nil ||
		!errors.Is(replacementErr, server.ErrClickHouseRecoveryArchiveMarkerMismatch) {
		t.Fatalf(
			"split-volume same-alias replacement error = %v, want archive marker rejection",
			replacementErr,
		)
	}
	if operationIndex != beforeReplacementRestore+1 {
		t.Fatalf(
			"same-alias replacement operation index = %d, want %d",
			operationIndex,
			beforeReplacementRestore+1,
		)
	}
	fixture.requireOperationStatus(
		t,
		ctx,
		admin,
		operationIDs[beforeReplacementRestore],
		deploymentRecoveryRestoreCompleteStatus,
	)
	if !nativeRecoveryIntegrationDatabaseExists(t, ctx, admin, deploymentRecoveryDatabase) {
		t.Fatal("same-alias replacement left no fail-closed unreceipted canonical database")
	}
	var receiptRows uint64
	if err := admin.QueryRow(
		ctx,
		"SELECT count() FROM "+canonicalDatabase+".recovery_sets",
	).Scan(&receiptRows); err != nil {
		t.Fatalf("count same-alias replacement receipts: %v", err)
	}
	if receiptRows != 0 {
		t.Fatalf("same-alias replacement receipt rows = %d, want zero", receiptRows)
	}
}

type nativeRecoveryIntegrationFixture struct {
	image             string
	containerName     string
	volumeName        string
	dataVolumeName    string
	logsVolumeName    string
	bootstrapNames    []string
	address           string
	recoveryConfig    string
	accessConfig      string
	bootstrapConfig   string
	bootstrapUsername string
	bootstrapPassword string
	backupPassword    string
	restorePassword   string
}

func startNativeRecoveryIntegrationClickHouse(
	t *testing.T,
	ctx context.Context,
	image string,
) *nativeRecoveryIntegrationFixture {
	t.Helper()
	suffix := nativeRecoveryIntegrationRandomHex(t, 6)
	fixture := &nativeRecoveryIntegrationFixture{
		image:             image,
		containerName:     "open-splunk-recovery-integration-" + suffix,
		volumeName:        "open-splunk-recovery-integration-" + suffix,
		dataVolumeName:    "open-splunk-recovery-integration-data-" + suffix,
		logsVolumeName:    "open-splunk-recovery-integration-logs-" + suffix,
		bootstrapUsername: "open_splunk_test_bootstrap",
		bootstrapPassword: nativeRecoveryIntegrationRandomHex(t, 32),
		backupPassword:    nativeRecoveryIntegrationRandomHex(t, 32),
		restorePassword:   nativeRecoveryIntegrationRandomHex(t, 32),
	}
	t.Cleanup(func() { fixture.close(t) })
	for _, volume := range []string{
		fixture.volumeName,
		fixture.dataVolumeName,
		fixture.logsVolumeName,
	} {
		fixture.docker(t, ctx, "volume", "create", volume)
	}

	for _, initializer := range []struct {
		name       string
		entrypoint string
		arguments  []string
	}{
		{
			name:       fixture.containerName + "-chown",
			entrypoint: "chown",
			arguments:  []string{"101:65532", nativeRecoveryIntegrationArchiveRoot},
		},
		{
			name:       fixture.containerName + "-chmod",
			entrypoint: "chmod",
			arguments:  []string{"2750", nativeRecoveryIntegrationArchiveRoot},
		},
	} {
		fixture.bootstrapNames = append(fixture.bootstrapNames, initializer.name)
		arguments := []string{
			"run", "--rm", "--name", initializer.name,
			"--user", "0:0",
			"--volume", fixture.volumeName + ":" + nativeRecoveryIntegrationArchiveRoot,
			"--entrypoint", initializer.entrypoint,
			fixture.image,
		}
		arguments = append(arguments, initializer.arguments...)
		fixture.docker(t, ctx, arguments...)
	}

	configDirectory := t.TempDir()
	fixture.bootstrapConfig = filepath.Join(configDirectory, "bootstrap-user.xml")
	const bootstrapXML = `<clickhouse>
    <users>
        <default remove="remove"/>
        <open_splunk_test_bootstrap>
            <password from_env="CLICKHOUSE_PASSWORD"/>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </open_splunk_test_bootstrap>
    </users>
</clickhouse>
`
	// #nosec G306 -- this generated nonsecret server config must be readable by
	// the unprivileged ClickHouse process in a rootful Linux container.
	if err := os.WriteFile(fixture.bootstrapConfig, []byte(bootstrapXML), 0o644); err != nil {
		t.Fatalf("write recovery integration bootstrap config: %v", err)
	}
	var err error
	fixture.recoveryConfig, err = filepath.Abs(filepath.Join("..", "..", "deploy", "clickhouse-config", "recovery.xml"))
	if err != nil {
		t.Fatalf("resolve recovery config: %v", err)
	}
	fixture.accessConfig, err = filepath.Abs(filepath.Join("..", "..", "deploy", "clickhouse-config", "access.xml"))
	if err != nil {
		t.Fatalf("resolve access config: %v", err)
	}
	for _, path := range []string{fixture.recoveryConfig, fixture.accessConfig} {
		if info, statErr := os.Stat(path); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("required ClickHouse recovery config %q is not a regular file: %v", path, statErr)
		}
	}
	fixture.startContainer(t, ctx, false)
	return fixture
}

func (fixture *nativeRecoveryIntegrationFixture) startContainer(
	t *testing.T,
	ctx context.Context,
	recoveryReadOnly bool,
) {
	t.Helper()
	recoveryMount := fixture.volumeName + ":" + nativeRecoveryIntegrationArchiveRoot
	if recoveryReadOnly {
		recoveryMount += ":ro"
	}
	fixture.docker(t, ctx,
		"run", "--detach", "--name", fixture.containerName,
		"--user", "101:101", "--group-add", "65532",
		"--publish", "127.0.0.1::9000",
		"--volume", fixture.dataVolumeName+":/var/lib/clickhouse",
		"--volume", fixture.logsVolumeName+":/var/log/clickhouse-server",
		"--volume", recoveryMount,
		"--volume", fixture.recoveryConfig+":"+
			"/etc/clickhouse-server/config.d/open_splunk_recovery.xml:ro",
		"--volume", fixture.accessConfig+":"+
			"/etc/clickhouse-server/config.d/open_splunk_access.xml:ro",
		"--volume", fixture.bootstrapConfig+":"+
			"/etc/clickhouse-server/users.d/open_splunk_test_bootstrap.xml:ro",
		"--env", "CLICKHOUSE_PASSWORD="+fixture.bootstrapPassword,
		"--env", "CLICKHOUSE_SKIP_USER_SETUP=1",
		fixture.image,
	)
	port := strings.TrimSpace(fixture.docker(t, ctx, "port", fixture.containerName, "9000/tcp"))
	if lines := strings.Fields(port); len(lines) != 1 || !strings.HasPrefix(lines[0], "127.0.0.1:") {
		t.Fatalf("recovery integration published address = %q, want one loopback address", port)
	} else {
		fixture.address = lines[0]
	}

	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		connection, openErr := clickhousedriver.Open(&clickhousedriver.Options{
			Protocol: clickhousedriver.Native,
			Addr:     []string{fixture.address},
			Auth: clickhousedriver.Auth{
				Database: "default",
				Username: fixture.bootstrapUsername,
				Password: fixture.bootstrapPassword,
			},
			DialTimeout:  3 * time.Second,
			MaxOpenConns: 1,
			MaxIdleConns: 1,
		})
		last = openErr
		if openErr == nil {
			probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
			last = connection.Ping(probeCtx)
			probeCancel()
			_ = connection.Close()
		}
		if last == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for recovery integration ClickHouse: %v", ctx.Err())
		case <-deadline.C:
			t.Fatalf("wait for recovery integration ClickHouse: %v", last)
		case <-ticker.C:
		}
	}
}

func (fixture *nativeRecoveryIntegrationFixture) restartContainer(
	t *testing.T,
	ctx context.Context,
	recoveryReadOnly bool,
) {
	t.Helper()
	fixture.docker(t, ctx, "container", "stop", "--timeout", "30", fixture.containerName)
	fixture.docker(t, ctx, "container", "rm", "--volumes", fixture.containerName)
	fixture.startContainer(t, ctx, recoveryReadOnly)
}

func (fixture *nativeRecoveryIntegrationFixture) open(
	t *testing.T,
	username string,
	password string,
) clickhousedriver.Conn {
	t.Helper()
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{fixture.address},
		Auth: clickhousedriver.Auth{
			Database: "default",
			Username: username,
			Password: password,
		},
		DialTimeout:     5 * time.Second,
		ReadTimeout:     2 * time.Minute,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: 3 * time.Minute,
	})
	if err != nil {
		t.Fatalf("open ClickHouse %s recovery session: %v", username, err)
	}
	return connection
}

func (fixture *nativeRecoveryIntegrationFixture) provisionRecoveryPrincipals(
	t *testing.T,
	ctx context.Context,
	admin clickhousedriver.Conn,
) {
	t.Helper()
	statements := []string{
		fmt.Sprintf(
			"CREATE USER %s IDENTIFIED WITH sha256_password BY '%s'",
			deploymentRecoveryBackupUsername,
			fixture.backupPassword,
		),
		"GRANT BACKUP, SHOW TABLES ON open_splunk.* TO " + deploymentRecoveryBackupUsername,
		"GRANT SELECT ON open_splunk.schema_migrations TO " + deploymentRecoveryBackupUsername,
		"GRANT SELECT(visibility_seq) ON open_splunk.events TO " + deploymentRecoveryBackupUsername,
		"GRANT INSERT, SELECT, TRUNCATE ON open_splunk.recovery_archive_markers TO " + deploymentRecoveryBackupUsername,
		"GRANT SELECT ON system.databases TO " + deploymentRecoveryBackupUsername,
		"GRANT SELECT ON system.mutations TO " + deploymentRecoveryBackupUsername,
		"GRANT SELECT ON system.tables TO " + deploymentRecoveryBackupUsername,
		fmt.Sprintf(
			"CREATE USER %s IDENTIFIED WITH sha256_password BY '%s'",
			deploymentRecoveryRestoreUsername,
			fixture.restorePassword,
		),
		"GRANT CREATE DATABASE, SHOW TABLES ON open_splunk.* TO " + deploymentRecoveryRestoreUsername,
		"GRANT CREATE TABLE, INSERT, SELECT(visibility_seq) ON open_splunk.events TO " + deploymentRecoveryRestoreUsername,
		"GRANT CREATE TABLE, INSERT, SELECT(name, version) ON open_splunk.schema_migrations TO " + deploymentRecoveryRestoreUsername,
		"GRANT CREATE TABLE, INSERT, SELECT, TRUNCATE ON open_splunk.recovery_archive_markers TO " + deploymentRecoveryRestoreUsername,
		"GRANT CREATE TABLE, INSERT, SELECT(database_uuid, deployment_manifest_sha256, events_table_uuid, recovery_archive_markers_table_uuid, recovery_set_id, recovery_sets_table_uuid, restored_at, schema_migrations_table_uuid, slot), TRUNCATE ON open_splunk.recovery_sets TO " + deploymentRecoveryRestoreUsername,
		"GRANT SELECT ON system.databases TO " + deploymentRecoveryRestoreUsername,
		"GRANT SELECT ON system.disks TO " + deploymentRecoveryRestoreUsername,
		"GRANT SELECT ON system.mutations TO " + deploymentRecoveryRestoreUsername,
		"GRANT SELECT ON system.tables TO " + deploymentRecoveryRestoreUsername,
		"GRANT SHOW DATABASES ON *.* TO " + deploymentRecoveryRestoreUsername,
	}
	for index, statement := range statements {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("provision recovery principal statement %d: %v", index+1, err)
		}
	}
}

func (fixture *nativeRecoveryIntegrationFixture) requireArchiveMetadata(
	t *testing.T,
	ctx context.Context,
	archiveName string,
) {
	t.Helper()
	metadata := strings.TrimSpace(fixture.docker(
		t,
		ctx,
		"exec", fixture.containerName,
		"stat", "-c", "%u:%g:%a:%s", filepath.Join(nativeRecoveryIntegrationArchiveRoot, archiveName),
	))
	parts := strings.Split(metadata, ":")
	if len(parts) != 4 || strings.Join(parts[:3], ":") != "101:65532:640" {
		t.Fatalf("native archive %s metadata = %q, want 101:65532:640:<positive-size>", archiveName, metadata)
	}
	size, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || size == 0 {
		t.Fatalf("native archive %s size metadata = %q, want positive bytes", archiveName, parts[3])
	}
}

func (fixture *nativeRecoveryIntegrationFixture) archiveIdentity(
	t *testing.T,
	ctx context.Context,
	archiveName string,
) recoveryset.FileIdentity {
	t.Helper()
	archivePath := filepath.Join(nativeRecoveryIntegrationArchiveRoot, archiveName)
	sizeText := strings.TrimSpace(fixture.docker(
		t,
		ctx,
		"exec", fixture.containerName,
		"stat", "-c", "%s", archivePath,
	))
	size, err := strconv.ParseUint(sizeText, 10, 64)
	if err != nil || size == 0 {
		t.Fatalf("native archive %s size = %q, want positive bytes", archiveName, sizeText)
	}
	digestFields := strings.Fields(fixture.docker(
		t,
		ctx,
		"exec", fixture.containerName,
		"sha256sum", archivePath,
	))
	if len(digestFields) != 2 || len(digestFields[0]) != 64 ||
		strings.Trim(digestFields[0], "0123456789abcdef") != "" {
		t.Fatalf("native archive %s SHA-256 output = %q", archiveName, digestFields)
	}
	return recoveryset.FileIdentity{
		Name:      archiveName,
		SizeBytes: size,
		SHA256:    digestFields[0],
	}
}

func (fixture *nativeRecoveryIntegrationFixture) requireOperationStatus(
	t *testing.T,
	ctx context.Context,
	admin clickhousedriver.Conn,
	operationID uuid.UUID,
	wantStatus string,
) {
	t.Helper()
	var count uint64
	var status string
	var operationError string
	if err := admin.QueryRow(ctx, `
		SELECT count(), any(toString(status)), any(error)
		FROM system.backups
		WHERE id = ?`, operationID.String()).Scan(&count, &status, &operationError); err != nil {
		t.Fatalf("inspect native operation %s: %v", operationID, err)
	}
	if count != 1 || status != wantStatus || operationError != "" {
		t.Fatalf(
			"native operation %s = count:%d status:%q error:%q, want one %s",
			operationID,
			count,
			status,
			operationError,
			wantStatus,
		)
	}
}

func (fixture *nativeRecoveryIntegrationFixture) operationCount(
	t *testing.T,
	ctx context.Context,
	admin clickhousedriver.Conn,
) uint64 {
	t.Helper()
	var count uint64
	if err := admin.QueryRow(ctx, "SELECT count() FROM system.backups").Scan(&count); err != nil {
		t.Fatalf("count native recovery operations: %v", err)
	}
	return count
}

func (fixture *nativeRecoveryIntegrationFixture) requireOperationFailure(
	t *testing.T,
	ctx context.Context,
	admin clickhousedriver.Conn,
	operationID uuid.UUID,
	wantStatus string,
) {
	t.Helper()
	var count uint64
	var status string
	var operationError string
	if err := admin.QueryRow(ctx, `
		SELECT count(), any(toString(status)), any(error)
		FROM system.backups
		WHERE id = ?`, operationID.String()).Scan(&count, &status, &operationError); err != nil {
		t.Fatalf("inspect failed native operation %s: %v", operationID, err)
	}
	if count != 1 || status != wantStatus || strings.TrimSpace(operationError) == "" {
		t.Fatalf(
			"failed native operation %s = count:%d status:%q error:%q, want one %s with an error",
			operationID,
			count,
			status,
			operationError,
			wantStatus,
		)
	}
}

func (fixture *nativeRecoveryIntegrationFixture) swapArchives(
	t *testing.T,
	ctx context.Context,
	first string,
	second string,
) {
	t.Helper()
	temporary := ".swap-" + nativeRecoveryIntegrationRandomHex(t, 6)
	root := nativeRecoveryIntegrationArchiveRoot
	for _, rename := range [][2]string{
		{filepath.Join(root, first), filepath.Join(root, temporary)},
		{filepath.Join(root, second), filepath.Join(root, first)},
		{filepath.Join(root, temporary), filepath.Join(root, second)},
	} {
		fixture.docker(t, ctx, "exec", fixture.containerName, "mv", rename[0], rename[1])
	}
}

func (fixture *nativeRecoveryIntegrationFixture) docker(
	t *testing.T,
	ctx context.Context,
	arguments ...string,
) string {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", arguments[0], err, nativeRecoveryIntegrationBoundedOutput(output))
	}
	return string(output)
}

func (fixture *nativeRecoveryIntegrationFixture) close(t *testing.T) {
	t.Helper()
	for _, name := range append([]string{fixture.containerName}, fixture.bootstrapNames...) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		output, err := exec.CommandContext(
			ctx,
			"docker",
			"rm",
			"--force",
			"--volumes",
			name,
		).CombinedOutput()
		cancel()
		if err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove recovery integration container %s: %v: %s", name, err, nativeRecoveryIntegrationBoundedOutput(output))
		}
	}
	for _, volume := range []string{
		fixture.volumeName,
		fixture.dataVolumeName,
		fixture.logsVolumeName,
	} {
		if volume == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		output, err := exec.CommandContext(ctx, "docker", "volume", "rm", volume).CombinedOutput()
		cancel()
		if err != nil && !strings.Contains(string(output), "no such volume") {
			t.Errorf("remove recovery integration volume %s: %v: %s", volume, err, nativeRecoveryIntegrationBoundedOutput(output))
		}
	}
}

func nativeRecoveryIntegrationBackupRequest(recoverySetID string) recoveryset.NativeBackupRequest {
	archiveName := recoverySetID + deploymentRecoveryArchiveSuffix
	return recoveryset.NativeBackupRequest{
		RecoverySetID:   recoverySetID,
		Disk:            deploymentRecoveryDisk,
		Database:        deploymentRecoveryDatabase,
		ArchiveDatabase: deploymentRecoveryArchiveDatabasePrefix + recoverySetID,
		ArchiveName:     archiveName,
		ArchivePath:     filepath.Join(nativeRecoveryIntegrationArchiveRoot, archiveName),
	}
}

func nativeRecoveryIntegrationVerification(
	recoverySetID string,
	manifestHash string,
	identity recoveryset.NativeBackupIdentity,
) recoveryset.Verification {
	return recoveryset.Verification{
		Manifest: recoveryset.Manifest{
			RecoverySetID: recoverySetID,
			ClickHouseMigrations: controlbackup.MigrationIdentity{
				LatestVersion: 1,
			},
			ClickHouse: recoveryset.ClickHouseIdentity{
				ServerVersion:                   identity.ServerVersion,
				Disk:                            deploymentRecoveryDisk,
				Database:                        deploymentRecoveryDatabase,
				ArchiveDatabase:                 deploymentRecoveryArchiveDatabasePrefix + recoverySetID,
				Archive:                         recoveryset.FileIdentity{Name: recoverySetID + deploymentRecoveryArchiveSuffix},
				MigrationLedgerSHA256:           identity.MigrationLedgerSHA256,
				DatabaseUUID:                    identity.DatabaseUUID,
				SchemaMigrationsTableUUID:       identity.SchemaMigrationsTableUUID,
				EventsTableUUID:                 identity.EventsTableUUID,
				RecoverySetsTableUUID:           identity.RecoverySetsTableUUID,
				RecoveryArchiveMarkersTableUUID: identity.RecoveryArchiveMarkersTableUUID,
				MaxVisibilitySeq:                identity.MaxVisibilitySeq,
				BackupOperationUUID:             identity.BackupOperationUUID,
			},
		},
		ManifestSHA256: manifestHash,
	}
}

func nativeRecoveryIntegrationGrants(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) (statements []string) {
	t.Helper()
	rows, err := connection.Query(ctx, "SHOW GRANTS")
	if err != nil {
		t.Fatalf("show recovery principal grants: %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Errorf("close SHOW GRANTS rows: %v", closeErr)
		}
	}()
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			t.Fatalf("scan SHOW GRANTS row: %v", err)
		}
		statements = append(statements, statement)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SHOW GRANTS rows: %v", err)
	}
	return statements
}

func nativeRecoveryIntegrationRequirePrivilegeDenied(
	t *testing.T,
	operation string,
	call func() error,
) {
	t.Helper()
	err := call()
	if err == nil {
		t.Fatalf("%s unexpectedly succeeded", operation)
	}
	var exception *clickhousedriver.Exception
	if !errors.As(err, &exception) || exception.Code != 497 {
		t.Fatalf("%s error = %v, want ClickHouse ACCESS_DENIED", operation, err)
	}
}

func nativeRecoveryIntegrationRequireMigrationLedgerBounds(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	const databaseName = "open_splunk_recovery_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := connection.Exec(ctx, "DROP DATABASE IF EXISTS "+databaseName+" SYNC"); err != nil {
		t.Fatalf("drop stale migration-bound fixture: %v", err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := connection.Exec(
			cleanupContext,
			"DROP DATABASE IF EXISTS "+databaseName+" SYNC",
		); err != nil {
			t.Errorf("drop migration-bound fixture: %v", err)
		}
	}()
	if err := connection.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create migration-bound fixture database: %v", err)
	}
	if err := connection.Exec(ctx, `
		CREATE TABLE `+databaseName+`.schema_migrations
		(
			version UInt32,
			name String
		)
		ENGINE = MergeTree
		ORDER BY version`); err != nil {
		t.Fatalf("create migration-bound fixture ledger: %v", err)
	}
	if err := connection.Exec(ctx, `
		INSERT INTO `+databaseName+`.schema_migrations
		SELECT toUInt32(number + 1), concat('overflow_', toString(number))
		FROM numbers(10000)`); err != nil {
		t.Fatalf("seed migration group overflow: %v", err)
	}
	_, err := server.ValidateClickHouseMigrationLedger(
		ctx,
		connection,
		migrations.ClickHouse(),
		databaseName,
	)
	nativeRecoveryIntegrationRequireBoundedReadFailure(
		t,
		"migration-ledger group sentinel",
		err,
		server.ErrClickHouseMigrationDrift,
	)

	if err := connection.Exec(ctx, "TRUNCATE TABLE "+databaseName+".schema_migrations SYNC"); err != nil {
		t.Fatalf("truncate group-sentinel fixture ledger: %v", err)
	}
	if err := connection.Exec(ctx, `
		INSERT INTO `+databaseName+`.schema_migrations
		SELECT toUInt32(1), 'duplicate_heavy'
		FROM numbers(10001)`); err != nil {
		t.Fatalf("seed migration duplicate-row overflow: %v", err)
	}
	_, err = server.ValidateClickHouseMigrationLedger(
		ctx,
		connection,
		migrations.ClickHouse(),
		databaseName,
	)
	nativeRecoveryIntegrationRequireBoundedReadFailure(
		t,
		"migration-ledger duplicate-row sentinel",
		err,
		server.ErrClickHouseMigrationDrift,
	)

	if err := connection.Exec(ctx, "TRUNCATE TABLE "+databaseName+".schema_migrations SYNC"); err != nil {
		t.Fatalf("truncate migration-bound fixture ledger: %v", err)
	}
	largeNameParts := make([]string, 0, 10)
	for range 9 {
		largeNameParts = append(largeNameParts, "repeat('x', 1000000)")
	}
	largeNameParts = append(largeNameParts, "repeat('x', 437184)")
	largeNameExpression := "concat(" + strings.Join(largeNameParts, ", ") + ")"
	if err := connection.Exec(
		ctx,
		"INSERT INTO "+databaseName+".schema_migrations "+
			"SELECT toUInt32(1), "+largeNameExpression,
	); err != nil {
		t.Fatalf("seed migration result-byte overflow: %v", err)
	}
	if _, err := server.ValidateClickHouseMigrationLedger(
		ctx,
		connection,
		migrations.ClickHouse(),
		databaseName,
	); err == nil {
		t.Fatal("migration-ledger result-byte overflow was accepted")
	} else {
		var exception *clickhousedriver.Exception
		if !errors.As(err, &exception) ||
			!strings.Contains(strings.ToLower(exception.Message), "result") {
			t.Fatalf("migration-ledger result-byte overflow error = %v, want ClickHouse result limit", err)
		}
	}

	for index := range 5 {
		if err := connection.Exec(
			ctx,
			fmt.Sprintf(
				"CREATE TABLE %s.extra_%d (value UInt8) ENGINE = Memory",
				databaseName,
				index,
			),
		); err != nil {
			t.Fatalf("create migration table-probe overflow fixture %d: %v", index, err)
		}
	}
	_, err = server.ValidateClickHouseMigrationLedger(
		ctx,
		connection,
		migrations.ClickHouse(),
		databaseName,
	)
	nativeRecoveryIntegrationRequireBoundedReadFailure(
		t,
		"migration system.tables sentinel",
		err,
		server.ErrClickHouseMigrationDrift,
	)
}

func nativeRecoveryIntegrationRequireSingletonReadBounds(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	const (
		recoverySetID = "dddddddddddddddddddddddddddddddd"
		manifestSHA   = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		operationID   = "60000000-0000-4000-8000-000000000001"
	)
	cleanup := func(cleanupContext context.Context) error {
		if err := connection.Exec(
			cleanupContext,
			"TRUNCATE TABLE open_splunk.recovery_archive_markers SYNC",
		); err != nil {
			return err
		}
		return connection.Exec(
			cleanupContext,
			"TRUNCATE TABLE open_splunk.recovery_sets SYNC",
		)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := cleanup(cleanupContext); err != nil {
			t.Errorf("clean singleton-bound fixtures: %v", err)
		}
	}()
	if err := cleanup(ctx); err != nil {
		t.Fatalf("initialize singleton-bound fixtures: %v", err)
	}

	if err := connection.Exec(ctx, `
		INSERT INTO open_splunk.recovery_archive_markers
			(slot, recovery_set_id, backup_operation_uuid)
		SELECT toUInt8(1), '`+recoverySetID+`', toUUID('`+operationID+`')
		FROM numbers(3)`); err != nil {
		t.Fatalf("seed archive-marker row overflow: %v", err)
	}
	err := server.RequireClickHouseRecoveryArchiveMarker(
		ctx,
		connection,
		deploymentRecoveryDatabase,
		recoverySetID,
		operationID,
	)
	nativeRecoveryIntegrationRequireBoundedReadFailure(
		t,
		"archive-marker raw-row sentinel",
		err,
		server.ErrClickHouseRecoveryArchiveMarkerMismatch,
	)
	if err := connection.Exec(
		ctx,
		"TRUNCATE TABLE open_splunk.recovery_archive_markers SYNC",
	); err != nil {
		t.Fatalf("truncate archive-marker row overflow: %v", err)
	}

	if err := connection.Exec(ctx, `
		INSERT INTO open_splunk.recovery_sets
		(
			slot,
			recovery_set_id,
			deployment_manifest_sha256,
			database_uuid,
			schema_migrations_table_uuid,
			events_table_uuid,
			recovery_sets_table_uuid,
			recovery_archive_markers_table_uuid,
			restored_at
		)
		SELECT
			toUInt8(1),
			'`+recoverySetID+`',
			'`+manifestSHA+`',
			toUUID('61000000-0000-4000-8000-000000000001'),
			toUUID('61000000-0000-4000-8000-000000000002'),
			toUUID('61000000-0000-4000-8000-000000000003'),
			toUUID('61000000-0000-4000-8000-000000000004'),
			toUUID('61000000-0000-4000-8000-000000000005'),
			now64(3)
		FROM numbers(3)`); err != nil {
		t.Fatalf("seed recovery-receipt row overflow: %v", err)
	}
	_, err = server.ReadClickHouseRecoveryReceipt(
		ctx,
		connection,
		deploymentRecoveryDatabase,
		recoverySetID,
		manifestSHA,
	)
	nativeRecoveryIntegrationRequireBoundedReadFailure(
		t,
		"recovery-receipt raw-row sentinel",
		err,
		server.ErrClickHouseRecoveryReceiptMismatch,
	)
}

func nativeRecoveryIntegrationRequireCatalogIsolation(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	const databaseName = "unrelated_catalog_noise"
	if err := connection.Exec(ctx, "DROP DATABASE IF EXISTS "+databaseName+" SYNC"); err != nil {
		t.Fatalf("drop stale unrelated catalog fixture: %v", err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := connection.Exec(
			cleanupContext,
			"DROP DATABASE IF EXISTS "+databaseName+" SYNC",
		); err != nil {
			t.Errorf("drop unrelated catalog fixture: %v", err)
		}
	}()
	if err := connection.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create unrelated catalog fixture: %v", err)
	}
	for index := range 16 {
		if err := connection.Exec(
			ctx,
			fmt.Sprintf(
				"CREATE TABLE %s.noise_%02d (value UInt8) ENGINE = Memory",
				databaseName,
				index,
			),
		); err != nil {
			t.Fatalf("create unrelated catalog table %d: %v", index, err)
		}
	}
	if err := server.ValidateClickHousePhysicalSchema(ctx, connection); err != nil {
		t.Fatalf("physical schema rejected unrelated catalog rows: %v", err)
	}
	if _, err := server.ValidateClickHouseMigrationLedger(
		ctx,
		connection,
		migrations.ClickHouse(),
		deploymentRecoveryDatabase,
	); err != nil {
		t.Fatalf("migration ledger rejected unrelated catalog rows: %v", err)
	}
}

func nativeRecoveryIntegrationRequireBoundedReadFailure(
	t *testing.T,
	operation string,
	err error,
	sentinel error,
) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s was accepted", operation)
	}
	if errors.Is(err, sentinel) {
		return
	}
	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) && exception.Code == 158 {
		return
	}
	t.Fatalf("%s error = %v, want fail-closed sentinel or ClickHouse TOO_MANY_ROWS_OR_BYTES", operation, err)
}

func nativeRecoveryIntegrationDatabaseExists(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	databaseName string,
) bool {
	t.Helper()
	var count uint64
	if err := connection.QueryRow(
		ctx,
		"SELECT count() FROM system.databases WHERE name = ?",
		databaseName,
	).Scan(&count); err != nil {
		t.Fatalf("inspect database %s existence: %v", databaseName, err)
	}
	if count > 1 {
		t.Fatalf("database %s count = %d, want zero or one", databaseName, count)
	}
	return count == 1
}

func nativeRecoveryIntegrationRandomHex(t *testing.T, byteCount int) string {
	t.Helper()
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate recovery integration identity: %v", err)
	}
	return hex.EncodeToString(value)
}

func nativeRecoveryIntegrationBoundedOutput(output []byte) string {
	const maximum = 8 << 10
	if len(output) <= maximum {
		return strings.TrimSpace(string(output))
	}
	return strings.TrimSpace(string(output[len(output)-maximum:]))
}
