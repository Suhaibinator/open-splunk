package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

const (
	deploymentRecoveryDrillArchiveRoot    = "/var/lib/open-splunk-clickhouse-backups"
	deploymentRecoveryDrillPrivateRoot    = "/var/lib/open-splunk/state/private"
	deploymentRecoveryDrillRecoverySource = "/var/lib/open-splunk/recovery/private/rehearsal-001"
	deploymentRecoveryDrillPendingJobID   = "recovery-pending"
	deploymentRecoveryDrillTenantID       = "default"
	deploymentRecoveryDrillOwnerID        = "42"
)

// TestDeploymentRecoveryDrillChild is an opt-in subprocess entry point for the
// disposable deployment drill. It is test-only so production recovery exposes
// no crash switches or fixture mutations.
func TestDeploymentRecoveryDrillChild(t *testing.T) {
	switch mode := os.Getenv("OPEN_SPLUNK_RECOVERY_DRILL_CHILD"); mode {
	case "":
		return
	case "seed-identity":
		seedDeploymentRecoveryDrillGoogleIdentity(t)
	case "create-empty-target":
		createDeploymentRecoveryDrillEmptyTarget(t)
	case "seed-pending":
		seedDeploymentRecoveryDrillPendingAttempt(t)
	case "crash-restore":
		crashDeploymentRecoveryDrillAfterReceipt(t)
	case "pause-after-receipt":
		t.Fatal(pauseDeploymentRecoveryDrillAfterReceipt(context.Background(), controlbackup.RestoreOptions{}))
	case "assert-target-absent":
		assertDeploymentRecoveryDrillTargetAbsent(t)
	default:
		t.Fatalf("unknown recovery drill child mode %q", mode)
	}
}

func assertDeploymentRecoveryDrillTargetAbsent(t *testing.T) {
	t.Helper()
	requireRecoverySchemaEmpty(t, os.Getenv("OPEN_SPLUNK_SERVER_POSTGRES_DSN"))
	for _, name := range []string{
		"master.key",
	} {
		path := filepath.Join(deploymentRecoveryDrillPrivateRoot, name)
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recovery target %q exists before control-plane publication: %v", path, err)
		}
	}
}

func seedDeploymentRecoveryDrillPendingAttempt(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	database, err := control.Open(ctx, os.Getenv("OPEN_SPLUNK_SERVER_POSTGRES_DSN"))
	if err != nil {
		t.Fatalf("open drill control plane: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close drill control plane: %v", err)
		}
	}()
	objectOptions := recoveryDrillEnvironmentObjectOptions()
	objects, err := objectOptions.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := objects.Close(); err != nil {
			t.Error(err)
		}
	}()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := admitDeploymentRecoveryDrillPendingAttempt(
		ctx,
		database,
		deploymentRecoveryDrillPrivateRoot+"/search-artifacts",
		objects,
		now,
	); err != nil {
		t.Fatalf("seed drill pending search attempt: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_PENDING_SEEDED"); err != nil {
		t.Fatalf("report drill pending search attempt: %v", err)
	}
}

func admitDeploymentRecoveryDrillPendingAttempt(
	ctx context.Context,
	database *control.DB,
	artifactDirectory string,
	blobs searchartifacts.BlobStore,
	now time.Time,
) error {
	auditEvents, err := searchaudit.New(database, searchaudit.Options{
		CursorKey: []byte("deployment-recovery-drill-audit-cursor-key-v1"),
	})
	if err != nil {
		return fmt.Errorf("open drill search-attempt audit: %w", err)
	}
	history, err := searchhistory.New(database, searchhistory.Options{
		AuditAppender:             auditEvents,
		RequireSearchAttemptAudit: true,
		CursorKey:                 []byte("deployment-recovery-drill-cursor-key-v1"),
	})
	if err != nil {
		return fmt.Errorf("open drill search history: %w", err)
	}
	historyJournal, err := searchhistory.NewJobJournal(history)
	if err != nil {
		return fmt.Errorf("open drill search-history journal: %w", err)
	}
	artifacts, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB:              database.SQLDB(),
		Blobs:           blobs,
		Directory:       artifactDirectory,
		Clock:           func() time.Time { return now },
		CleanupInterval: -1,
	})
	if err != nil {
		return fmt.Errorf("open drill search artifacts: %w", err)
	}
	timeRange := searchtime.Intent{
		Earliest: "-15m",
		Latest:   "now",
		Timezone: "UTC",
	}
	job := searchjobs.Job{
		ID:               deploymentRecoveryDrillPendingJobID,
		Version:          1,
		OwnerID:          deploymentRecoveryDrillOwnerID,
		OrganizationID:   "1",
		SPL:              "index=main | head 1",
		TenantID:         deploymentRecoveryDrillTenantID,
		RequestedIndexes: []string{"main"},
		EffectiveIndexes: []string{"main"},
		TimeRange:        timeRange,
		AppID:            "search",
		Source:           searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc},
		Earliest:         now.Add(-15 * time.Minute),
		Latest:           now,
		IndexTimeCutoff:  now,
		State:            searchjobs.StateQueued,
		CreatedAt:        now,
	}
	admitErr := searchjobs.NewCompositeJournal(artifacts, historyJournal).Admit(ctx, job)
	return errors.Join(admitErr, artifacts.Close())
}

func crashDeploymentRecoveryDrillAfterReceipt(t *testing.T) {
	t.Helper()
	release, err := loadControlPlaneRecoveryRelease()
	if err != nil {
		t.Fatalf("load drill recovery release: %v", err)
	}
	flags := flag.NewFlagSet("drill-object-configuration", flag.ContinueOnError)
	var objectOptions recoveryObjectOptions
	registerRecoveryObjectFlags(flags, &objectOptions)
	dependencies := defaultDeploymentRecoveryDependencies()
	dependencies.restoreControlPlane = pauseDeploymentRecoveryDrillAfterReceipt
	err = runRestoreDeploymentRecoverySetWithDependencies(
		context.Background(),
		deploymentRecoveryRestoreOptions{
			Source:                deploymentRecoveryDrillRecoverySource,
			recoveryObjectOptions: objectOptions,
			ArchiveRoot:           deploymentRecoveryDrillArchiveRoot,
			PostgresDSN:           consumeRecoverySecret("OPEN_SPLUNK_SERVER_POSTGRES_DSN"),
			MasterKeyPath:         deploymentRecoveryDrillPrivateRoot + "/master.key",
			Address:               "clickhouse:9440",
			PasswordFile:          "/run/recovery/restore.password",
			CACertFile:            "/run/recovery/ca.crt",
			ServerName:            "clickhouse",
		},
		release,
		dependencies,
	)
	if err == nil {
		t.Fatal("crash drill restore returned before the parent terminated it")
	}
	t.Fatalf("crash drill restore ended before parent termination: %v", err)
}

func pauseDeploymentRecoveryDrillAfterReceipt(ctx context.Context, _ controlbackup.RestoreOptions) error {
	// The native session has closed at this boundary. Keep a timer live while
	// the parent inspects the receipt and sends SIGKILL: a static helper with
	// only a nil Done channel can otherwise exit through Go's deadlock detector.
	// Bound this test-only pause by the same budget as the parent drill.
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_RECEIPT_PUBLISHED"); err != nil {
		return fmt.Errorf("report drill recovery receipt: %w", err)
	}
	<-ctx.Done()
	return ctx.Err()
}

func recoveryDrillEnvironmentObjectOptions() recoveryObjectOptions {
	flags := flag.NewFlagSet("drill-objects", flag.ContinueOnError)
	var options recoveryObjectOptions
	registerRecoveryObjectFlags(flags, &options)
	return options
}

// Seed only this disposable test database with a deliberately promoted Google
// account. Production OAuth registration still creates no memberships/admins.
func seedDeploymentRecoveryDrillGoogleIdentity(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	db, err := control.Open(ctx, os.Getenv("OPEN_SPLUNK_SERVER_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	objectOptions := recoveryDrillEnvironmentObjectOptions()
	opts := options{tenantID: deploymentRecoveryDrillTenantID, masterKeyPath: deploymentRecoveryDrillPrivateRoot + "/master.key", publicBaseURL: os.Getenv("OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL"), googleClientID: os.Getenv("OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID"), googleClientSecret: os.Getenv("OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET"), minioEndpoint: objectOptions.Endpoint, minioAccessKey: objectOptions.AccessKey, minioSecretKey: objectOptions.SecretKey, minioBucket: objectOptions.Bucket, minioUseTLS: objectOptions.UseTLS}
	runtime, err := newRuntimeIdentity(ctx, db, opts, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runtime.users.Close(); err != nil {
			t.Error(err)
		}
	}()
	user := pm.User{ID: 42, Username: "recovery-google", Email: "recovery-google@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10}
	if err := db.GORMDB().WithContext(ctx).Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GORMDB().WithContext(ctx).Create(&pm.OAuthLogin{UserID: user.ID, ProviderSlug: "google", Subject: new("recovery-test-subject"), Email: user.Email}).Error; err != nil {
		t.Fatal(err)
	}
	master, err := loadVerifiedMasterKey(ctx, db, opts.masterKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(master)
	secret, err := deriveServerKey(master, "gocommon-session-signing")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(secret)
	tokens, err := token.NewTokenService(runtime.users.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: hex.EncodeToString(secret)}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	session, err := tokens.GenerateSessionToken(ctx, user.ID, token.SessionAuthMethod{Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/identity/organizations/create-with-owner", strings.NewReader(`{"name":"Recovery drill","slug":"recovery-drill","owner_user_id":42}`))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(&http.Cookie{Name: "open_splunk_session", Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	response := httptest.NewRecorder()
	runtime.users.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"organization_id":1`) {
		t.Fatalf("create disposable owner organization status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := fmt.Fprintln(os.Stdout, "RECOVERY_GOOGLE_SESSION="+session); err != nil {
		t.Fatal(err)
	}
}

func createDeploymentRecoveryDrillEmptyTarget(t *testing.T) {
	t.Helper()
	connection, err := pgx.Connect(t.Context(), os.Getenv("OPEN_SPLUNK_SERVER_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connection.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	if _, err := connection.Exec(t.Context(), `CREATE DATABASE restored TEMPLATE template0`); err != nil {
		t.Fatal(err)
	}
}
