//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const releaseOCIIntegrationFlag = "OPEN_SPLUNK_OCI_INTEGRATION"

// TestReleaseOCIComposeContract is the release packaging acceptance boundary.
// It is opt-in because it builds both production OCI targets and starts the
// digest-pinned full-stack Compose deployment with persistent named volumes.
func TestReleaseOCIComposeContract(t *testing.T) {
	if os.Getenv(releaseOCIIntegrationFlag) != "1" {
		t.Skip("set " + releaseOCIIntegrationFlag + "=1 to run the release OCI integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker is required when %s=1: %v", releaseOCIIntegrationFlag, err)
	}
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Fatalf("release OCI integration does not support host architecture %q", runtime.GOARCH)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	repository := repositoryRoot(t)
	deployDirectory := filepath.Join(repository, "deploy")
	releaseOCIRequireCompose(t, ctx, repository)
	releaseOCIRequirePinnedClickHouse(t)

	envFile := filepath.Join(t.TempDir(), "deployment.env")
	values := releaseOCIGenerateEnvironment(t, ctx, deployDirectory, envFile)
	suffix := releaseOCIRandomHex(t, 6)
	serverImage := "open-splunk-oci-integration:" + suffix + "-server"
	collectorImage := "open-splunk-oci-integration:" + suffix + "-collector"
	values["OPEN_SPLUNK_SERVER_IMAGE"] = serverImage
	values["OPEN_SPLUNK_COLLECTOR_IMAGE"] = collectorImage
	values["OPEN_SPLUNK_OCI_PLATFORM"] = "linux/" + runtime.GOARCH
	// Let Docker reserve both host ports atomically while it creates the
	// container. Bind-and-close probing leaves an avoidable port-stealing
	// window while the two release images build.
	values["OPEN_SPLUNK_SERVER_HTTP_PORT"] = "0"
	values["OPEN_SPLUNK_SERVER_GRPC_PORT"] = "0"
	values["OPEN_SPLUNK_HEC_ENABLED"] = "true"
	values["COMPOSE_ANSI"] = "never"
	values["COMPOSE_PROGRESS"] = "plain"

	stack := &releaseOCIComposeStack{
		project:         "open-splunk-oci-test-" + suffix,
		repository:      repository,
		deployDirectory: deployDirectory,
		composeFile:     filepath.Join(deployDirectory, "docker-compose.yaml"),
		envFile:         envFile,
		values:          values,
		serverImage:     serverImage,
		collectorImage:  collectorImage,
	}
	t.Cleanup(func() { stack.cleanup(t) })

	releaseOCIBuildImages(t, ctx, stack)
	releaseOCIAssertImageContract(t, ctx, stack, serverImage, collectorImage)
	releaseOCIAssertProductionComposeConfig(t, ctx, stack)

	stack.mustCompose(
		t,
		ctx,
		"start release Compose deployment",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
	)

	serverID := stack.serviceContainerID(t, ctx, "server", false)
	clickHouseID := stack.serviceContainerID(t, ctx, "clickhouse", false)
	migratorID := stack.serviceContainerID(t, ctx, "clickhouse-migrator", true)
	recoveryVolumeBootstrapID := stack.serviceContainerID(
		t,
		ctx,
		"clickhouse-recovery-volume-bootstrap",
		true,
	)
	bootstrapID := stack.serviceContainerID(t, ctx, "server-bootstrap", true)
	serverContainer := releaseOCIInspectContainer(t, ctx, stack, serverID)
	clickHouseContainer := releaseOCIInspectContainer(t, ctx, stack, clickHouseID)
	migratorContainer := releaseOCIInspectContainer(t, ctx, stack, migratorID)
	recoveryVolumeBootstrapContainer := releaseOCIInspectContainer(
		t,
		ctx,
		stack,
		recoveryVolumeBootstrapID,
	)
	bootstrapContainer := releaseOCIInspectContainer(t, ctx, stack, bootstrapID)
	releaseOCIAssertContainerHardening(t, serverContainer, "65532:65532", true)
	releaseOCIAssertContainerHardening(t, migratorContainer, "65532:65532", false)
	releaseOCIAssertContainerHardening(t, bootstrapContainer, "65532:65532", false)
	releaseOCIAssertContainerHardening(t, clickHouseContainer, "101:101", true)
	releaseOCIAssertContainerHealth(t, serverContainer, "server")
	releaseOCIAssertContainerHealth(t, clickHouseContainer, "clickhouse")
	releaseOCIAssertMigratorCompleted(t, migratorContainer)
	releaseOCIAssertMigratorIsolation(t, stack, migratorContainer)
	releaseOCIAssertRecoveryVolumeBootstrap(t, stack, recoveryVolumeBootstrapContainer)
	releaseOCIAssertBootstrapCompleted(t, bootstrapContainer)
	releaseOCIAssertBootstrapNetworkDisabled(t, bootstrapContainer)
	releaseOCIAssertClickHouseHasNoHostPorts(t, clickHouseContainer)
	releaseOCIAssertClickHouseRecoveryVolumeOwnership(t, ctx, stack)
	releaseOCIAssertDefaultClickHouseUserRejected(t, ctx, stack)
	releaseOCIAssertServerEnvironmentHasNoSecrets(t, stack, serverContainer)
	httpAddress, grpcAddress := releaseOCIPublishedServerAddresses(t, serverContainer)
	if httpAddress == grpcAddress {
		t.Fatalf("release server HTTP and gRPC share host address %q", httpAddress)
	}

	client, baseURL := releaseOCIHTTPSClient(t, stack, httpAddress)
	releaseOCIAssertHTTPSBoundary(t, ctx, client, baseURL, httpAddress)
	releaseOCIAssertRuntimeReadinessFailureRecovery(
		t,
		ctx,
		stack,
		client,
		baseURL,
		clickHouseID,
	)
	releaseOCIAssertEmbeddedRelease(t, ctx, client, baseURL, stack)

	administratorToken := strings.TrimSpace(releaseOCIReadFile(
		t,
		values["OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"],
	))
	if administratorToken == "" {
		t.Fatal("generated administrator token is empty")
	}
	firstIndex := "oci-before-recreate-" + suffix
	releaseOCICreateIndex(t, ctx, client, baseURL, administratorToken, firstIndex)
	firstStateVolume := releaseOCIStateVolume(t, serverContainer)

	// A release migration is restart-safe and validates the existing ledger as
	// an exact match before exiting. This uses the image's embedded migrations,
	// never a checkout bind mount.
	stack.mustCompose(
		t,
		ctx,
		"reapply exact release migrations",
		"run",
		"--rm",
		"--no-deps",
		"clickhouse-migrator",
	)

	releaseOCIRotateClickHouseCredentials(t, stack)

	stack.mustCompose(
		t,
		ctx,
		"force-recreate deployment with coordinated ClickHouse credential rotation",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
		"--force-recreate",
		"clickhouse-recovery-volume-bootstrap",
		"clickhouse",
		"clickhouse-migrator",
		"server-bootstrap",
		"server",
	)

	recreatedServerID := stack.serviceContainerID(t, ctx, "server", false)
	if recreatedServerID == serverID {
		t.Fatalf("force-recreate retained server container %q", serverID)
	}
	recreatedClickHouseID := stack.serviceContainerID(t, ctx, "clickhouse", false)
	if recreatedClickHouseID == clickHouseID {
		t.Fatalf("force-recreate retained ClickHouse container %q", clickHouseID)
	}
	recreatedMigratorID := stack.serviceContainerID(t, ctx, "clickhouse-migrator", true)
	if recreatedMigratorID == migratorID {
		t.Fatalf("force-recreate retained ClickHouse migrator container %q", migratorID)
	}
	recreatedRecoveryVolumeBootstrapID := stack.serviceContainerID(
		t,
		ctx,
		"clickhouse-recovery-volume-bootstrap",
		true,
	)
	if recreatedRecoveryVolumeBootstrapID == recoveryVolumeBootstrapID {
		t.Fatalf(
			"force-recreate retained ClickHouse recovery volume bootstrap container %q",
			recoveryVolumeBootstrapID,
		)
	}
	recreatedBootstrapID := stack.serviceContainerID(t, ctx, "server-bootstrap", true)
	recreatedServerContainer := releaseOCIInspectContainer(t, ctx, stack, recreatedServerID)
	recreatedClickHouseContainer := releaseOCIInspectContainer(
		t,
		ctx,
		stack,
		recreatedClickHouseID,
	)
	recreatedMigratorContainer := releaseOCIInspectContainer(
		t,
		ctx,
		stack,
		recreatedMigratorID,
	)
	recreatedRecoveryVolumeBootstrapContainer := releaseOCIInspectContainer(
		t,
		ctx,
		stack,
		recreatedRecoveryVolumeBootstrapID,
	)
	recreatedBootstrapContainer := releaseOCIInspectContainer(t, ctx, stack, recreatedBootstrapID)
	if got := releaseOCIStateVolume(t, recreatedServerContainer); got != firstStateVolume {
		t.Fatalf("server state volume after force-recreate = %q, want %q", got, firstStateVolume)
	}
	releaseOCIAssertBootstrapCompleted(t, recreatedBootstrapContainer)
	releaseOCIAssertBootstrapNetworkDisabled(t, recreatedBootstrapContainer)
	releaseOCIAssertRecoveryVolumeBootstrap(
		t,
		stack,
		recreatedRecoveryVolumeBootstrapContainer,
	)
	releaseOCIAssertContainerHealth(t, recreatedClickHouseContainer, "recreated ClickHouse")
	releaseOCIAssertClickHouseRecoveryVolumeOwnership(t, ctx, stack)
	releaseOCIAssertContainerHardening(t, recreatedMigratorContainer, "65532:65532", false)
	releaseOCIAssertMigratorCompleted(t, recreatedMigratorContainer)
	releaseOCIAssertMigratorIsolation(t, stack, recreatedMigratorContainer)
	releaseOCIAssertContainerHealth(t, recreatedServerContainer, "recreated server")
	releaseOCIAssertServerEnvironmentHasNoSecrets(t, stack, recreatedServerContainer)
	recreatedHTTPAddress, recreatedGRPCAddress := releaseOCIPublishedServerAddresses(
		t,
		recreatedServerContainer,
	)
	if recreatedHTTPAddress == recreatedGRPCAddress {
		t.Fatalf(
			"recreated release server HTTP and gRPC share host address %q",
			recreatedHTTPAddress,
		)
	}
	client.CloseIdleConnections()
	client, baseURL = releaseOCIHTTPSClient(t, stack, recreatedHTTPAddress)
	releaseOCIAssertHTTPSBoundary(
		t,
		ctx,
		client,
		baseURL,
		recreatedHTTPAddress,
	)

	secondIndex := "oci-after-recreate-" + suffix
	releaseOCICreateIndex(t, ctx, client, baseURL, administratorToken, secondIndex)
	bootstrap := releaseOCIBootstrap(t, ctx, client, baseURL)
	for _, name := range []string{firstIndex, secondIndex} {
		if !slices.ContainsFunc(bootstrap.GetIndexes(), func(index *opensplunkv1.IndexSummary) bool {
			return index.GetName() == name
		}) {
			t.Fatalf("persisted bootstrap indexes do not contain %q: %+v", name, bootstrap.GetIndexes())
		}
	}
	recoveryFixture := &releaseOCIRecoveryFixture{
		t:                    t,
		ctx:                  ctx,
		stack:                stack,
		client:               client,
		baseURL:              baseURL,
		grpcAddress:          recreatedGRPCAddress,
		administratorToken:   administratorToken,
		preBackupIndexes:     []string{firstIndex, secondIndex},
		originalStateVolume:  firstStateVolume,
		originalServerID:     recreatedServerID,
		originalClickHouseID: recreatedClickHouseID,
		bootstrapID:          recreatedBootstrapID,
		suffix:               suffix,
	}
	recoveryFixture.run()
}

const (
	releaseOCIClickHouseDataPath     = "/var/lib/clickhouse"
	releaseOCIClickHouseLogsPath     = "/var/log/clickhouse-server"
	releaseOCIClickHouseRecoveryPath = "/var/lib/open-splunk-clickhouse-backups"
	releaseOCIServerExportsPath      = "/var/lib/open-splunk/exports"
)

type releaseOCIRecoveryFixture struct {
	t     *testing.T
	ctx   context.Context
	stack *releaseOCIComposeStack

	client               *http.Client
	baseURL              string
	grpcAddress          string
	administratorToken   string
	preBackupIndexes     []string
	originalStateVolume  string
	originalServerID     string
	originalClickHouseID string
	bootstrapID          string
	suffix               string

	recoveryIndex              string
	credential                 releaseOCIIngestionCredential
	fixtureTime                time.Time
	preBackupEventID           string
	postBackupEventID          string
	postRestoreEventID         string
	postBackupIndex            string
	mutationServerID           string
	originalClickHouseRecovery string
	restoredClient             *http.Client
	restoredBaseURL            string
	restoredGRPCAddress        string
	hecRecovery                *releaseOCIHECRecoveryState
}

type releaseOCIRecoveryHelperMount struct {
	kind     string
	source   string
	writable bool
}

type releaseOCIRecoveryHelperContract struct {
	command     []string
	mounts      map[string]releaseOCIRecoveryHelperMount
	environment []string
	networkMode string
	networks    []string
	tmpfs       bool
	user        string
	workingDir  string
	pidsLimit   int64
	exitCode    int
}

func releaseOCIRunRecoveryHelper(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	purpose string,
	service string,
	stateVolume string,
	recoveryVolume string,
) {
	t.Helper()
	contract := releaseOCIRecoveryHelperContractFor(
		t,
		stack,
		service,
		stateVolume,
		recoveryVolume,
	)
	containerName := stack.project + "-" + purpose + "-" + releaseOCIRandomHex(t, 4)
	stack.ownedContainers = append(stack.ownedContainers, containerName)
	stack.mustCompose(
		t,
		ctx,
		"run and retain "+service+" for confinement inspection",
		"--profile",
		"recovery",
		"run",
		"--name",
		containerName,
		"--no-deps",
		service,
	)
	container := releaseOCIInspectContainer(t, ctx, stack, containerName)
	releaseOCIAssertRecoveryHelperConfinement(t, stack, service, container, contract)
	releaseOCIRunDocker(
		t,
		ctx,
		stack,
		"remove inspected "+service+" one-off container",
		"container",
		"rm",
		"--force",
		"--volumes",
		containerName,
	)
	stack.forgetOwnedContainer(containerName)
}

func releaseOCIRequireRecoveryHelperFailure(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	purpose string,
	service string,
	stateVolume string,
	recoveryVolume string,
	wantError string,
) {
	t.Helper()
	contract := releaseOCIRecoveryHelperContractFor(
		t,
		stack,
		service,
		stateVolume,
		recoveryVolume,
	)
	contract.exitCode = 1
	containerName := stack.project + "-" + purpose + "-" + releaseOCIRandomHex(t, 4)
	stack.ownedContainers = append(stack.ownedContainers, containerName)
	output, truncated, err := stack.runCompose(
		ctx,
		"--profile",
		"recovery",
		"run",
		"--name",
		containerName,
		"--no-deps",
		service,
	)
	if err == nil || truncated || !strings.Contains(output, wantError) {
		t.Fatalf(
			"%s failure = %v truncated=%t output=%q, want error containing %q",
			service,
			err,
			truncated,
			stack.redact(output),
			wantError,
		)
	}
	container := releaseOCIInspectContainer(t, ctx, stack, containerName)
	releaseOCIAssertRecoveryHelperConfinement(t, stack, service, container, contract)
	releaseOCIRunDocker(
		t,
		ctx,
		stack,
		"remove inspected failing "+service+" one-off container",
		"container",
		"rm",
		"--force",
		"--volumes",
		containerName,
	)
	stack.forgetOwnedContainer(containerName)
}

func releaseOCIRecoveryHelperContractFor(
	t *testing.T,
	stack *releaseOCIComposeStack,
	service string,
	stateVolume string,
	recoveryVolume string,
) releaseOCIRecoveryHelperContract {
	t.Helper()
	recoverySetPath := releaseOCIRecoverySetPath(stack)
	serverRecoveryVolume := stack.project + "_server-recovery"
	serverLockVolume := stack.project + "_server-lock"
	backendNetwork := stack.project + "_backend"
	contract := releaseOCIRecoveryHelperContract{
		networkMode: backendNetwork,
		networks:    []string{backendNetwork},
		user:        "65532:65532",
		workingDir:  "/var/lib/open-splunk/state/private",
		pidsLimit:   64,
		environment: []string{
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		},
	}
	switch service {
	case "deployment-backup":
		if stateVolume == "" || recoveryVolume == "" {
			t.Fatal("deployment-backup confinement contract requires exact state and recovery volumes")
		}
		contract.command = []string{
			"backup-deployment-recovery-set",
			"-control-db",
			"/var/lib/open-splunk/state/private/open-splunk.db",
			"-master-key",
			"/var/lib/open-splunk/state/private/master.key",
			"-administrator-token-file",
			"/var/lib/open-splunk/state/private/administrator.token",
			"-destination",
			recoverySetPath,
			"-archive-root",
			"/var/lib/open-splunk/clickhouse-backups",
			"-address",
			"clickhouse:9440",
			"-password-file",
			"/run/open-splunk/clickhouse/backup.password",
			"-ca-cert",
			"/run/open-splunk/clickhouse/ca.crt",
			"-server-name",
			stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		}
		contract.mounts = map[string]releaseOCIRecoveryHelperMount{
			"/var/lib/open-splunk/state": {
				kind: "volume", source: stateVolume, writable: true,
			},
			"/var/lib/open-splunk/lock": {
				kind: "volume", source: serverLockVolume, writable: true,
			},
			"/var/lib/open-splunk/recovery": {
				kind: "volume", source: serverRecoveryVolume, writable: true,
			},
			"/var/lib/open-splunk/clickhouse-backups": {
				kind: "volume", source: recoveryVolume,
			},
			"/run/open-splunk/clickhouse/ca.crt": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"],
			},
			"/run/open-splunk/clickhouse/backup.password": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE"],
			},
		}
		contract.environment = append(
			contract.environment,
			"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH=/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock",
		)
		contract.tmpfs = true
	case "deployment-verify":
		if stateVolume != "" || recoveryVolume == "" {
			t.Fatal("deployment-verify confinement contract requires only the recovery volume")
		}
		contract.command = []string{
			"verify-deployment-recovery-set",
			"-source",
			recoverySetPath,
			"-archive-root",
			"/var/lib/open-splunk/clickhouse-backups",
		}
		contract.mounts = map[string]releaseOCIRecoveryHelperMount{
			"/var/lib/open-splunk/recovery": {
				kind: "volume", source: serverRecoveryVolume,
			},
			"/var/lib/open-splunk/clickhouse-backups": {
				kind: "volume", source: recoveryVolume,
			},
		}
		contract.networkMode = "none"
		contract.networks = []string{"none"}
	case "deployment-restore":
		if stateVolume == "" || recoveryVolume == "" {
			t.Fatal("deployment-restore confinement contract requires exact state and recovery volumes")
		}
		contract.command = []string{
			"restore-deployment-recovery-set",
			"-source",
			recoverySetPath,
			"-archive-root",
			"/var/lib/open-splunk/clickhouse-backups",
			"-control-db",
			"/var/lib/open-splunk/state/private/open-splunk.db",
			"-master-key",
			"/var/lib/open-splunk/state/private/master.key",
			"-administrator-token-file",
			"/var/lib/open-splunk/state/private/administrator.token",
			"-address",
			"clickhouse:9440",
			"-password-file",
			"/run/open-splunk/clickhouse/restore.password",
			"-ca-cert",
			"/run/open-splunk/clickhouse/ca.crt",
			"-server-name",
			stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		}
		contract.mounts = map[string]releaseOCIRecoveryHelperMount{
			"/var/lib/open-splunk/state": {
				kind: "volume", source: stateVolume, writable: true,
			},
			"/var/lib/open-splunk/lock": {
				kind: "volume", source: serverLockVolume, writable: true,
			},
			"/var/lib/open-splunk/recovery": {
				kind: "volume", source: serverRecoveryVolume,
			},
			"/var/lib/open-splunk/clickhouse-backups": {
				kind: "volume", source: recoveryVolume,
			},
			"/run/open-splunk/clickhouse/ca.crt": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"],
			},
			"/run/open-splunk/clickhouse/restore.password": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE"],
			},
		}
		contract.environment = append(
			contract.environment,
			"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH=/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock",
		)
		contract.tmpfs = true
	case "deployment-marker-reconcile":
		if stateVolume != "" || recoveryVolume != "" {
			t.Fatal("deployment-marker-reconcile confinement contract requires no state or recovery volume")
		}
		recoverySetID := stack.values["OPEN_SPLUNK_STALE_RECOVERY_SET_ID"]
		confirmedRecoverySetID := stack.values["OPEN_SPLUNK_CONFIRMED_STALE_RECOVERY_SET_ID"]
		backupOperationUUID := stack.values["OPEN_SPLUNK_STALE_BACKUP_OPERATION_UUID"]
		confirmedBackupOperationUUID := stack.values["OPEN_SPLUNK_CONFIRMED_STALE_BACKUP_OPERATION_UUID"]
		if recoverySetID == "" || confirmedRecoverySetID == "" ||
			backupOperationUUID == "" || confirmedBackupOperationUUID == "" {
			t.Fatal("deployment-marker-reconcile confinement contract requires explicit marker identities")
		}
		contract.command = []string{
			"reconcile-deployment-recovery-marker",
			"-recovery-set-id",
			recoverySetID,
			"-confirm-recovery-set-id",
			confirmedRecoverySetID,
			"-backup-operation-uuid",
			backupOperationUUID,
			"-confirm-backup-operation-uuid",
			confirmedBackupOperationUUID,
			"-address",
			"clickhouse:9440",
			"-password-file",
			"/run/open-splunk/clickhouse/backup.password",
			"-ca-cert",
			"/run/open-splunk/clickhouse/ca.crt",
			"-server-name",
			stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		}
		contract.mounts = map[string]releaseOCIRecoveryHelperMount{
			"/var/lib/open-splunk/lock": {
				kind: "volume", source: serverLockVolume, writable: true,
			},
			"/run/open-splunk/clickhouse/ca.crt": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"],
			},
			"/run/open-splunk/clickhouse/backup.password": {
				kind: "bind", source: stack.values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE"],
			},
		}
		contract.environment = append(
			contract.environment,
			"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH=/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock",
		)
		contract.tmpfs = true
	case "deployment-archive-delete":
		if stateVolume != "" || recoveryVolume == "" {
			t.Fatal("deployment-archive-delete confinement contract requires only the recovery volume")
		}
		archiveName := stack.values["OPEN_SPLUNK_FAILED_RECOVERY_ARCHIVE_NAME"]
		confirmedName := stack.values["OPEN_SPLUNK_CONFIRMED_RECOVERY_ARCHIVE_NAME"]
		if archiveName == "" || confirmedName != archiveName {
			t.Fatal("deployment-archive-delete confinement contract requires matching exact archive confirmations")
		}
		contract.command = []string{
			"delete-deployment-recovery-archive",
			"-archive-root",
			"/var/lib/open-splunk/clickhouse-backups",
			"-archive-name",
			archiveName,
			"-confirm-archive-name",
			confirmedName,
		}
		contract.mounts = map[string]releaseOCIRecoveryHelperMount{
			"/var/lib/open-splunk/clickhouse-backups": {
				kind: "volume", source: recoveryVolume, writable: true,
			},
		}
		contract.networkMode = "none"
		contract.networks = []string{"none"}
		contract.user = "101:65532"
		contract.workingDir = "/"
		contract.pidsLimit = 32
	default:
		t.Fatalf("unsupported recovery helper service %q", service)
	}
	return contract
}

func releaseOCIRecoverySetPath(stack *releaseOCIComposeStack) string {
	path := stack.values["OPEN_SPLUNK_DEPLOYMENT_RECOVERY_SET_PATH"]
	if path == "" {
		path = os.Getenv("OPEN_SPLUNK_DEPLOYMENT_RECOVERY_SET_PATH")
	}
	if path == "" {
		return "/var/lib/open-splunk/recovery/private/deployment-recovery-set"
	}
	return path
}

func releaseOCIAssertRecoveryHelperConfinement(
	t *testing.T,
	stack *releaseOCIComposeStack,
	service string,
	container *releaseOCIContainerInspect,
	contract releaseOCIRecoveryHelperContract,
) {
	t.Helper()
	releaseOCIAssertContainerHardening(t, container, contract.user, false)
	if !slices.Equal(container.HostConfig.CapDrop, []string{"ALL"}) ||
		!slices.Equal(container.HostConfig.SecurityOpt, []string{"no-new-privileges:true"}) {
		t.Fatalf(
			"%s one-off capability/security contract = cap-drop %v security %v, want exactly ALL/no-new-privileges",
			service,
			container.HostConfig.CapDrop,
			container.HostConfig.SecurityOpt,
		)
	}
	if container.HostConfig.Privileged || container.HostConfig.PidMode != "" ||
		container.HostConfig.UTSMode != "" ||
		container.HostConfig.UsernsMode != "" ||
		(container.HostConfig.IpcMode != "" && container.HostConfig.IpcMode != "private") ||
		(container.HostConfig.CgroupnsMode != "" && container.HostConfig.CgroupnsMode != "private") ||
		len(container.HostConfig.Devices) != 0 || len(container.HostConfig.DeviceRequests) != 0 ||
		len(container.HostConfig.DeviceCgroupRules) != 0 ||
		len(container.HostConfig.GroupAdd) != 0 || len(container.HostConfig.VolumesFrom) != 0 {
		t.Fatalf(
			"%s one-off has privileged device/namespace sharing: privileged=%t pid=%q ipc=%q uts=%q userns=%q cgroup=%q devices=%d requests=%d device-rules=%v groups=%v volumes-from=%v",
			service,
			container.HostConfig.Privileged,
			container.HostConfig.PidMode,
			container.HostConfig.IpcMode,
			container.HostConfig.UTSMode,
			container.HostConfig.UsernsMode,
			container.HostConfig.CgroupnsMode,
			len(container.HostConfig.Devices),
			len(container.HostConfig.DeviceRequests),
			container.HostConfig.DeviceCgroupRules,
			container.HostConfig.GroupAdd,
			container.HostConfig.VolumesFrom,
		)
	}
	if container.State.Status != "exited" || container.State.ExitCode != contract.exitCode {
		t.Fatalf(
			"%s one-off state = %q exit %d, want retained exit %d",
			service,
			container.State.Status,
			container.State.ExitCode,
			contract.exitCode,
		)
	}
	if container.HostConfig.AutoRemove {
		t.Fatalf("%s one-off unexpectedly enables automatic removal", service)
	}
	if container.HostConfig.PidsLimit != contract.pidsLimit {
		t.Fatalf(
			"%s one-off PID limit = %d, want %d",
			service,
			container.HostConfig.PidsLimit,
			contract.pidsLimit,
		)
	}
	if container.Config.WorkingDir != contract.workingDir {
		t.Fatalf(
			"%s one-off working directory = %q, want %q",
			service,
			container.Config.WorkingDir,
			contract.workingDir,
		)
	}
	if !slices.Equal(
		container.Config.Entrypoint,
		[]string{"/usr/local/bin/open-splunk-server"},
	) || !slices.Equal(container.Config.Cmd, contract.command) {
		t.Fatalf(
			"%s one-off process = entrypoint %q command %q, want exact command %q",
			service,
			container.Config.Entrypoint,
			container.Config.Cmd,
			contract.command,
		)
	}
	gotEnvironment := slices.Clone(container.Config.Env)
	wantEnvironment := slices.Clone(contract.environment)
	slices.Sort(gotEnvironment)
	slices.Sort(wantEnvironment)
	if !slices.Equal(gotEnvironment, wantEnvironment) {
		t.Fatalf("%s one-off environment = %q, want exact %q", service, gotEnvironment, wantEnvironment)
	}
	if container.Config.Healthcheck != nil &&
		!slices.Equal(container.Config.Healthcheck.Test, []string{"NONE"}) {
		t.Fatalf(
			"%s one-off healthcheck = %q, want absent or disabled",
			service,
			container.Config.Healthcheck.Test,
		)
	}
	process := strings.Join(
		append(slices.Clone(container.Config.Entrypoint), container.Config.Cmd...),
		"\x00",
	)
	for _, secret := range stack.secrets() {
		if secret != "" && strings.Contains(process, secret) {
			t.Fatalf("%s one-off process arguments contain secret material", service)
		}
	}
	releaseOCIAssertRecoveryHelperMounts(t, service, container, contract.mounts)
	releaseOCIAssertRecoveryHelperTmpfs(t, service, container, contract.tmpfs)
	if container.HostConfig.NetworkMode != contract.networkMode ||
		!slices.Equal(mapsKeys(container.NetworkSettings.Networks), contract.networks) {
		t.Fatalf(
			"%s one-off network mode/networks = %q/%v, want %q/%v",
			service,
			container.HostConfig.NetworkMode,
			mapsKeys(container.NetworkSettings.Networks),
			contract.networkMode,
			contract.networks,
		)
	}
	if len(container.HostConfig.PortBindings) != 0 {
		t.Fatalf("%s one-off publishes host ports: %+v", service, container.HostConfig.PortBindings)
	}
	for port, bindings := range container.NetworkSettings.Ports {
		if len(bindings) != 0 {
			t.Fatalf("%s one-off port %s is published: %+v", service, port, bindings)
		}
	}
}

func releaseOCIAssertRecoveryHelperMounts(
	t *testing.T,
	service string,
	container *releaseOCIContainerInspect,
	want map[string]releaseOCIRecoveryHelperMount,
) {
	t.Helper()
	if len(container.Mounts) != len(want) {
		t.Fatalf("%s one-off mounts = %+v, want exact destinations %v", service, container.Mounts, mapsKeys(want))
	}
	seen := make(map[string]struct{}, len(container.Mounts))
	for _, mount := range container.Mounts {
		expected, exists := want[mount.Destination]
		if !exists {
			t.Fatalf("%s one-off has unexpected mount %+v", service, mount)
		}
		if _, duplicate := seen[mount.Destination]; duplicate {
			t.Fatalf("%s one-off has duplicate mount destination %q", service, mount.Destination)
		}
		seen[mount.Destination] = struct{}{}
		if mount.Type != expected.kind || mount.RW != expected.writable {
			t.Fatalf("%s one-off mount = %+v, want type %q writable=%t", service, mount, expected.kind, expected.writable)
		}
		switch expected.kind {
		case "volume":
			if mount.Name != expected.source {
				t.Fatalf("%s one-off volume mount = %+v, want volume %q", service, mount, expected.source)
			}
		case "bind":
			if mount.Name != "" || !releaseOCIBindSourceMatches(mount.Source, expected.source) {
				t.Fatalf("%s one-off bind mount = %+v, want source %q", service, mount, expected.source)
			}
		default:
			t.Fatalf("%s one-off expected mount has unsupported type %q", service, expected.kind)
		}
	}
}

func releaseOCIBindSourceMatches(actual, expected string) bool {
	return releaseOCIBindSourceMatchesForOS(runtime.GOOS, actual, expected)
}

func releaseOCIBindSourceMatchesForOS(hostOS, actual, expected string) bool {
	if !filepath.IsAbs(actual) || !filepath.IsAbs(expected) ||
		filepath.Clean(actual) != actual || filepath.Clean(expected) != expected {
		return false
	}
	actualHostPath := actual
	// Docker Desktop exposes macOS host bind sources under /host_mnt inside
	// its Linux VM. Remove only that fixed transport prefix, then resolve both
	// host paths so /var and /private/var aliases compare by physical identity.
	if hostOS == "darwin" && strings.HasPrefix(actualHostPath, "/host_mnt/") {
		actualHostPath = strings.TrimPrefix(actualHostPath, "/host_mnt")
	}
	physicalActual, actualErr := filepath.EvalSymlinks(actualHostPath)
	physicalExpected, expectedErr := filepath.EvalSymlinks(expected)
	return actualErr == nil && expectedErr == nil && physicalActual == physicalExpected
}

func TestReleaseOCIBindSourceIdentity(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	if !releaseOCIBindSourceMatchesForOS("darwin", "/host_mnt"+physical, path) {
		t.Fatal("Darwin Docker Desktop physical bind identity was rejected")
	}
	if releaseOCIBindSourceMatchesForOS("linux", "/host_mnt"+physical, path) {
		t.Fatal("Linux bind identity accepted the Docker Desktop transport prefix")
	}
	if releaseOCIBindSourceMatchesForOS("darwin", "/host_mnt"+physical+"-other", path) {
		t.Fatal("different Darwin Docker Desktop bind source was accepted")
	}
}

func releaseOCIAssertRecoveryHelperTmpfs(
	t *testing.T,
	service string,
	container *releaseOCIContainerInspect,
	want bool,
) {
	t.Helper()
	if !want {
		if len(container.HostConfig.Tmpfs) != 0 {
			t.Fatalf("%s one-off tmpfs mounts = %+v, want none", service, container.HostConfig.Tmpfs)
		}
		return
	}
	options, exists := container.HostConfig.Tmpfs["/tmp"]
	if len(container.HostConfig.Tmpfs) != 1 || !exists {
		t.Fatalf("%s one-off tmpfs mounts = %+v, want only /tmp", service, container.HostConfig.Tmpfs)
	}
	got := strings.Split(options, ",")
	wantOptions := []string{"gid=65532", "mode=0700", "nodev", "noexec", "nosuid", "rw", "uid=65532"}
	slices.Sort(got)
	slices.Sort(wantOptions)
	if !slices.Equal(got, wantOptions) {
		t.Fatalf("%s one-off /tmp options = %v, want %v", service, got, wantOptions)
	}
}

func (fixture *releaseOCIRecoveryFixture) run() {
	fixture.t.Helper()
	fixture.seedPreBackupState()
	fixture.seedHECPreBackupState()
	fixture.captureBackupBoundaryAndPostBackupMutations()
	fixture.restoreIntoFreshVolumes()
	fixture.assertPostRestoreState()
}

func (fixture *releaseOCIRecoveryFixture) seedPreBackupState() {
	t := fixture.t
	t.Helper()

	fixture.recoveryIndex = "oci-recovery-" + fixture.suffix
	fixture.preBackupIndexes = append(
		slices.Clone(fixture.preBackupIndexes),
		fixture.recoveryIndex,
	)
	releaseOCICreateIndex(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		fixture.administratorToken,
		fixture.recoveryIndex,
	)
	collectorID := "oci-recovery-collector-" + fixture.suffix
	fixture.credential = releaseOCICreateIngestionCredential(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		fixture.administratorToken,
		"Release recovery authentication proof",
		fixture.recoveryIndex,
		collectorID,
	)
	fixture.stack.retainedSecrets = append(
		fixture.stack.retainedSecrets,
		fixture.credential.plaintext,
	)
	fixture.fixtureTime = time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Millisecond)
	fixture.preBackupEventID = "oci-recovery-before-backup-" + fixture.suffix
	fixture.postBackupEventID = "oci-recovery-after-backup-" + fixture.suffix
	fixture.postRestoreEventID = "oci-recovery-after-restore-" + fixture.suffix
	releaseOCIIngestEvent(
		t,
		fixture.ctx,
		fixture.stack,
		fixture.grpcAddress,
		fixture.credential,
		fixture.recoveryIndex,
		fixture.preBackupEventID,
		"paired recovery pre-backup event",
		fixture.fixtureTime,
		1,
	)
	releaseOCIAssertSearchEventIDs(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		fixture.recoveryIndex,
		fixture.fixtureTime,
		[]string{fixture.preBackupEventID},
	)
}

func (fixture *releaseOCIRecoveryFixture) captureBackupBoundaryAndPostBackupMutations() {
	t := fixture.t
	t.Helper()
	ctx := fixture.ctx
	stack := fixture.stack

	fixture.stagePendingHECAndStopServer()
	fixture.client.CloseIdleConnections()
	stoppedServer := releaseOCIInspectContainer(t, ctx, stack, fixture.originalServerID)
	if stoppedServer.State.Status != "exited" {
		t.Fatalf("server state before deployment backup = %q, want exited", stoppedServer.State.Status)
	}
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"paired-backup",
		"deployment-backup",
		fixture.originalStateVolume,
		stack.project+"_clickhouse-recovery",
	)
	releaseOCIAssertClickHouseArchiveMarkerAbsent(
		t,
		ctx,
		stack,
		"live canonical source after backup",
	)
	fixture.exerciseStaleMarkerReconciliation()
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"paired-verify",
		"deployment-verify",
		"",
		stack.project+"_clickhouse-recovery",
	)

	// Prove the recovery set is a point-in-time boundary by committing one
	// control-plane mutation and the next visibility-reserved batch afterward.
	stack.mustCompose(
		t,
		ctx,
		"restart original server for post-backup mutations",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
		"--no-deps",
		"server",
	)
	fixture.mutationServerID = stack.serviceContainerID(t, ctx, "server", false)
	if fixture.mutationServerID != fixture.originalServerID {
		t.Fatalf(
			"post-backup mutation restart replaced server %q with %q",
			fixture.originalServerID,
			fixture.mutationServerID,
		)
	}
	mutationServer := releaseOCIInspectContainer(t, ctx, stack, fixture.mutationServerID)
	if got := releaseOCIStateVolume(t, mutationServer); got != fixture.originalStateVolume {
		t.Fatalf(
			"post-backup mutation server state volume = %q, want %q",
			got,
			fixture.originalStateVolume,
		)
	}
	releaseOCIAssertContainerHardening(t, mutationServer, "65532:65532", true)
	releaseOCIAssertContainerHealth(t, mutationServer, "post-backup mutation server")
	postBackupHTTPAddress, postBackupGRPCAddress := releaseOCIPublishedServerAddresses(
		t,
		mutationServer,
	)
	if postBackupHTTPAddress == postBackupGRPCAddress {
		t.Fatalf(
			"post-backup mutation server HTTP and gRPC share host address %q",
			postBackupHTTPAddress,
		)
	}
	postBackupClient, postBackupBaseURL := releaseOCIHTTPSClient(
		t,
		stack,
		postBackupHTTPAddress,
	)
	releaseOCIAssertHTTPSBoundary(
		t,
		ctx,
		postBackupClient,
		postBackupBaseURL,
		postBackupHTTPAddress,
	)
	fixture.recordPostBackupHECMutation(postBackupClient, postBackupBaseURL)
	fixture.postBackupIndex = "oci-after-backup-" + fixture.suffix
	releaseOCICreateIndex(
		t,
		ctx,
		postBackupClient,
		postBackupBaseURL,
		fixture.administratorToken,
		fixture.postBackupIndex,
	)
	releaseOCIIngestEvent(
		t,
		ctx,
		stack,
		postBackupGRPCAddress,
		fixture.credential,
		fixture.recoveryIndex,
		fixture.postBackupEventID,
		"paired recovery post-backup event",
		fixture.fixtureTime.Add(time.Second),
		2,
	)
	releaseOCIAssertSearchEventIDs(
		t,
		ctx,
		postBackupClient,
		postBackupBaseURL,
		fixture.recoveryIndex,
		fixture.fixtureTime,
		[]string{fixture.preBackupEventID, fixture.postBackupEventID},
	)
	lockNegativeStateVolume := releaseOCICreateRecoveryVolume(
		t,
		ctx,
		stack,
		"lock-negative-server-state",
	)
	lockNegativeOverridePath := filepath.Join(
		filepath.Dir(stack.envFile),
		"lock-negative-state.override.yaml",
	)
	lockNegativeOverride := "volumes:\n" +
		"  server-state:\n    external: true\n    name: " +
		strconv.Quote(lockNegativeStateVolume) + "\n"
	if err := os.WriteFile(lockNegativeOverridePath, []byte(lockNegativeOverride), 0o600); err != nil {
		t.Fatalf("write singleton-lock negative Compose override: %v", err)
	}
	stack.composeOverrides = append(stack.composeOverrides, lockNegativeOverridePath)
	releaseOCIRequireRecoveryHelperFailure(
		t,
		ctx,
		stack,
		"restore-while-server-running",
		"deployment-restore",
		lockNegativeStateVolume,
		stack.project+"_clickhouse-recovery",
		"lock /var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock is held",
	)
	stack.composeOverrides = stack.composeOverrides[:len(stack.composeOverrides)-1]

	originalClickHouse := releaseOCIInspectContainer(t, ctx, stack, fixture.originalClickHouseID)
	originalClickHouseData := releaseOCIVolumeAt(t, originalClickHouse, releaseOCIClickHouseDataPath)
	originalClickHouseLogs := releaseOCIVolumeAt(t, originalClickHouse, releaseOCIClickHouseLogsPath)
	fixture.originalClickHouseRecovery = releaseOCIVolumeAt(
		t,
		originalClickHouse,
		releaseOCIClickHouseRecoveryPath,
	)
	originalServerExports := releaseOCIVolumeAt(
		t,
		mutationServer,
		releaseOCIServerExportsPath,
	)
	stack.ownedVolumes = append(stack.ownedVolumes, []string{
		fixture.originalStateVolume,
		originalServerExports,
		originalClickHouseData,
		originalClickHouseLogs,
	}...)
	stack.mustCompose(t, ctx, "stop original deployment before fresh-volume restore", "stop", "--timeout", "40")
	fixture.client.CloseIdleConnections()
	postBackupClient.CloseIdleConnections()
	fixture.exerciseAttestedArchiveDeletion()
}

func (fixture *releaseOCIRecoveryFixture) exerciseStaleMarkerReconciliation() {
	t := fixture.t
	t.Helper()
	ctx := fixture.ctx
	stack := fixture.stack
	const (
		recoverySetID       = "dddddddddddddddddddddddddddddddd"
		wrongRecoverySetID  = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		backupOperationUUID = "70000000-0000-4000-8000-000000000001"
	)
	releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"seed interrupted-backup marker for packaged reconciliation",
		"INSERT INTO open_splunk.recovery_archive_markers (slot, recovery_set_id, backup_operation_uuid) VALUES (1, '"+
			recoverySetID+"', toUUID('"+backupOperationUUID+"'))",
	)
	stack.mustCompose(
		t,
		ctx,
		"stop ClickHouse to terminate any old native backup before marker reconciliation",
		"stop",
		"--timeout",
		"40",
		"clickhouse",
	)
	stack.mustCompose(
		t,
		ctx,
		"restart only ClickHouse before marker reconciliation",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
		"--no-deps",
		"clickhouse",
	)
	setMarkerIdentity := func(id string) {
		stack.values["OPEN_SPLUNK_STALE_RECOVERY_SET_ID"] = id
		stack.values["OPEN_SPLUNK_CONFIRMED_STALE_RECOVERY_SET_ID"] = id
		stack.values["OPEN_SPLUNK_STALE_BACKUP_OPERATION_UUID"] = backupOperationUUID
		stack.values["OPEN_SPLUNK_CONFIRMED_STALE_BACKUP_OPERATION_UUID"] = backupOperationUUID
	}
	setMarkerIdentity(wrongRecoverySetID)
	releaseOCIRequireRecoveryHelperFailure(
		t,
		ctx,
		stack,
		"marker-reconcile-mismatch",
		"deployment-marker-reconcile",
		"",
		"",
		"require exact confirmed marker",
	)
	exactMarkerQuery := "SELECT count() FROM open_splunk.recovery_archive_markers " +
		"WHERE recovery_set_id = '" + recoverySetID + "' AND backup_operation_uuid = toUUID('" +
		backupOperationUUID + "') FORMAT TSVRaw"
	if got := releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"prove mismatched reconciliation retained exact marker",
		exactMarkerQuery,
	); got != "1" {
		t.Fatalf("marker after mismatched reconciliation = %q, want exact retained row", got)
	}
	setMarkerIdentity(recoverySetID)
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"marker-reconcile-exact",
		"deployment-marker-reconcile",
		"",
		"",
	)
	releaseOCIAssertClickHouseArchiveMarkerAbsent(
		t,
		ctx,
		stack,
		"packaged exact marker reconciliation",
	)
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"marker-reconcile-idempotent-retry",
		"deployment-marker-reconcile",
		"",
		"",
	)
}

func (fixture *releaseOCIRecoveryFixture) exerciseAttestedArchiveDeletion() {
	t := fixture.t
	t.Helper()
	ctx := fixture.ctx
	stack := fixture.stack
	recoveryVolume := fixture.originalClickHouseRecovery
	if recoveryVolume == "" {
		t.Fatal("archive deletion drill requires the original ClickHouse recovery volume")
	}
	orphanName := releaseOCIRandomHex(t, 16) + ".tar.zst"
	releaseOCIRunArchiveVolumeShell(
		t,
		ctx,
		stack,
		recoveryVolume,
		"create test-owned failed-attempt archive",
		fmt.Sprintf(
			`set -eu; set -- /archive/*.tar.zst; test "$#" -eq 1; test -f "$1"; cp "$1" /archive/%s; chmod 0640 /archive/%s`,
			orphanName,
			orphanName,
		),
	)
	stack.values["OPEN_SPLUNK_FAILED_RECOVERY_ARCHIVE_NAME"] = orphanName
	stack.values["OPEN_SPLUNK_CONFIRMED_RECOVERY_ARCHIVE_NAME"] = orphanName
	defer delete(stack.values, "OPEN_SPLUNK_FAILED_RECOVERY_ARCHIVE_NAME")
	defer delete(stack.values, "OPEN_SPLUNK_CONFIRMED_RECOVERY_ARCHIVE_NAME")
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"attested-archive-delete",
		"deployment-archive-delete",
		"",
		recoveryVolume,
	)
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"attested-archive-delete-retry",
		"deployment-archive-delete",
		"",
		recoveryVolume,
	)
	releaseOCIRunArchiveVolumeShell(
		t,
		ctx,
		stack,
		recoveryVolume,
		"prove exact failed-attempt deletion retained the published archive",
		fmt.Sprintf(
			`set -eu; test ! -e /archive/%s; set -- /archive/*.tar.zst; test "$#" -eq 1; test -f "$1"`,
			orphanName,
		),
	)
}

func releaseOCIRunArchiveVolumeShell(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	volume string,
	operation string,
	script string,
) {
	t.Helper()
	containerName := stack.project + "-archive-volume-probe-" + releaseOCIRandomHex(t, 4)
	stack.ownedContainers = append(stack.ownedContainers, containerName)
	releaseOCIRunDocker(
		t,
		ctx,
		stack,
		operation,
		"run",
		"--rm",
		"--name",
		containerName,
		"--label",
		"com.open-splunk.integration.project="+stack.project,
		"--network",
		"none",
		"--user",
		"101:65532",
		"--workdir",
		"/",
		"--read-only",
		"--cap-drop",
		"ALL",
		"--security-opt",
		"no-new-privileges:true",
		"--pids-limit",
		"32",
		"--mount",
		"type=volume,source="+volume+",target=/archive",
		"--entrypoint",
		"/bin/sh",
		testsupport.DefaultClickHouseImage,
		"-eu",
		"-c",
		script,
	)
	stack.forgetOwnedContainer(containerName)
}

func (fixture *releaseOCIRecoveryFixture) restoreIntoFreshVolumes() {
	t := fixture.t
	t.Helper()
	ctx := fixture.ctx
	stack := fixture.stack

	restoredStateVolume := releaseOCICreateRecoveryVolume(t, ctx, stack, "restored-server-state")
	restoredServerExports := releaseOCICreateRecoveryVolume(t, ctx, stack, "restored-server-exports")
	restoredClickHouseData := releaseOCICreateRecoveryVolume(t, ctx, stack, "restored-clickhouse-data")
	restoredClickHouseLogs := releaseOCICreateRecoveryVolume(t, ctx, stack, "restored-clickhouse-logs")
	stack.values["OPEN_SPLUNK_RECOVERY_CLICKHOUSE_DATA_VOLUME"] = restoredClickHouseData
	stack.values["OPEN_SPLUNK_RECOVERY_CLICKHOUSE_LOGS_VOLUME"] = restoredClickHouseLogs
	stack.values["OPEN_SPLUNK_RECOVERY_SERVER_STATE_VOLUME"] = restoredStateVolume
	stack.values["OPEN_SPLUNK_RECOVERY_SERVER_EXPORTS_VOLUME"] = restoredServerExports
	stack.composeOverrides = append(
		stack.composeOverrides,
		filepath.Join(stack.deployDirectory, "docker-compose.recovery-target.yaml"),
	)
	stack.composeOverrides = append(
		stack.composeOverrides,
		filepath.Join(stack.deployDirectory, "docker-compose.restore.yaml"),
	)
	releaseOCIAssertRestoreComposeConfig(t, ctx, stack)

	// A fresh ClickHouse initializes only recovery principals. Neither the
	// migrator nor either server process may create schema before RESTORE.
	stack.mustCompose(
		t,
		ctx,
		"start only fresh ClickHouse before deployment restore",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
		"--no-deps",
		"--force-recreate",
		"clickhouse",
	)
	restoredClickHouseID := stack.serviceContainerID(t, ctx, "clickhouse", false)
	if restoredClickHouseID == fixture.originalClickHouseID {
		t.Fatalf(
			"fresh-volume recovery retained original ClickHouse container %q",
			fixture.originalClickHouseID,
		)
	}
	restoredClickHouse := releaseOCIInspectContainer(t, ctx, stack, restoredClickHouseID)
	if got := releaseOCIVolumeAt(t, restoredClickHouse, releaseOCIClickHouseDataPath); got != restoredClickHouseData {
		t.Fatalf("restored ClickHouse data volume = %q, want %q", got, restoredClickHouseData)
	}
	if got := releaseOCIVolumeAt(t, restoredClickHouse, releaseOCIClickHouseLogsPath); got != restoredClickHouseLogs {
		t.Fatalf("restored ClickHouse logs volume = %q, want %q", got, restoredClickHouseLogs)
	}
	if got := releaseOCIReadOnlyVolumeAt(
		t,
		restoredClickHouse,
		releaseOCIClickHouseRecoveryPath,
	); got != fixture.originalClickHouseRecovery {
		t.Fatalf(
			"restored ClickHouse recovery volume = %q, want retained %q",
			got,
			fixture.originalClickHouseRecovery,
		)
	}
	releaseOCIAssertContainerHealth(t, restoredClickHouse, "fresh ClickHouse before restore")
	releaseOCIAssertClickHouseRecoveryVolumeOwnership(t, ctx, stack)
	releaseOCIAssertClickHouseRecoveryNamespace(t, ctx, stack, "fresh restore target", "")
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"paired-restore",
		"deployment-restore",
		restoredStateVolume,
		fixture.originalClickHouseRecovery,
	)
	releaseOCIAssertRestoredClickHouseRecoveryIdentity(t, ctx, stack, "initial restore")
	releaseOCIRunRecoveryHelper(
		t,
		ctx,
		stack,
		"paired-restore-retry",
		"deployment-restore",
		restoredStateVolume,
		fixture.originalClickHouseRecovery,
	)
	releaseOCIAssertRestoredClickHouseRecoveryIdentity(t, ctx, stack, "restore retry")
	stack.mustCompose(
		t,
		ctx,
		"verify restored schema with exact embedded migrator",
		"run",
		"--rm",
		"--no-deps",
		"clickhouse-migrator",
	)
	if got := stack.serviceContainerID(t, ctx, "server-bootstrap", true); got != fixture.bootstrapID {
		t.Fatalf("deployment restore recreated server bootstrap %q as %q", fixture.bootstrapID, got)
	}

	// Start directly from restored state. Running server-bootstrap could create
	// a missing administrator token and mask an incomplete control restore.
	stack.mustCompose(
		t,
		ctx,
		"start server directly on restored state",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"240",
		"--no-build",
		"--no-deps",
		"--force-recreate",
		"server",
	)
	restoredServerID := stack.serviceContainerID(t, ctx, "server", false)
	if restoredServerID == fixture.mutationServerID {
		t.Fatalf("paired recovery retained pre-restore server container %q", fixture.mutationServerID)
	}
	restoredServer := releaseOCIInspectContainer(t, ctx, stack, restoredServerID)
	if got := releaseOCIStateVolume(t, restoredServer); got != restoredStateVolume {
		t.Fatalf("restored server state volume = %q, want %q", got, restoredStateVolume)
	}
	if got := releaseOCIVolumeAt(t, restoredServer, releaseOCIServerExportsPath); got != restoredServerExports {
		t.Fatalf("restored server exports volume = %q, want %q", got, restoredServerExports)
	}
	if got := stack.serviceContainerID(t, ctx, "server-bootstrap", true); got != fixture.bootstrapID {
		t.Fatalf("direct restored server start recreated bootstrap %q as %q", fixture.bootstrapID, got)
	}
	releaseOCIAssertContainerHardening(t, restoredServer, "65532:65532", true)
	releaseOCIAssertContainerHealth(t, restoredServer, "restored server")
	releaseOCIAssertServerEnvironmentHasNoSecrets(t, stack, restoredServer)
	restoredHTTPAddress, restoredGRPCAddress := releaseOCIPublishedServerAddresses(t, restoredServer)
	if restoredHTTPAddress == restoredGRPCAddress {
		t.Fatalf(
			"restored release server HTTP and gRPC share host address %q",
			restoredHTTPAddress,
		)
	}
	fixture.restoredGRPCAddress = restoredGRPCAddress
	fixture.restoredClient, fixture.restoredBaseURL = releaseOCIHTTPSClient(
		t,
		stack,
		restoredHTTPAddress,
	)
	releaseOCIAssertHTTPSBoundary(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		restoredHTTPAddress,
	)
	status, body, err := releaseOCIProbeHealthEndpoint(
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL+"/readyz",
	)
	if err != nil || status != http.StatusOK || body != "ok\n" {
		t.Fatalf(
			"restored runtime readiness = status %d body %q error %v",
			status,
			body,
			err,
		)
	}
}

func (fixture *releaseOCIRecoveryFixture) assertPostRestoreState() {
	t := fixture.t
	t.Helper()
	ctx := fixture.ctx
	stack := fixture.stack

	restoredBootstrap := releaseOCIBootstrap(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
	)
	for _, name := range fixture.preBackupIndexes {
		if !slices.ContainsFunc(restoredBootstrap.GetIndexes(), func(index *opensplunkv1.IndexSummary) bool {
			return index.GetName() == name
		}) {
			t.Fatalf("restored bootstrap indexes do not contain %q: %+v", name, restoredBootstrap.GetIndexes())
		}
	}
	if slices.ContainsFunc(restoredBootstrap.GetIndexes(), func(index *opensplunkv1.IndexSummary) bool {
		return index.GetName() == fixture.postBackupIndex
	}) {
		t.Fatalf(
			"restored bootstrap retained post-backup index %q: %+v",
			fixture.postBackupIndex,
			restoredBootstrap.GetIndexes(),
		)
	}
	releaseOCIAssertRestoredIngestionCredential(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.administratorToken,
		fixture.credential,
		fixture.recoveryIndex,
	)
	releaseOCIAssertSearchEventIDs(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.recoveryIndex,
		fixture.fixtureTime,
		[]string{fixture.preBackupEventID},
	)
	fixture.assertRestoredHECState()
	releaseOCIIngestEvent(
		t,
		ctx,
		stack,
		fixture.restoredGRPCAddress,
		fixture.credential,
		fixture.recoveryIndex,
		fixture.postRestoreEventID,
		"paired recovery post-restore visibility event",
		fixture.fixtureTime.Add(2*time.Second),
		2,
	)
	releaseOCIAssertSearchEventIDs(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.recoveryIndex,
		fixture.fixtureTime,
		[]string{fixture.preBackupEventID, fixture.postRestoreEventID},
	)

	postRestoreIndex := "oci-after-restore-" + fixture.suffix
	releaseOCICreateIndex(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.administratorToken,
		postRestoreIndex,
	)
	postRestoreCredential := releaseOCICreateIngestionCredential(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.administratorToken,
		"Release recovery post-restore write",
		postRestoreIndex,
		"oci-after-restore-collector-"+fixture.suffix,
	)
	stack.retainedSecrets = append(stack.retainedSecrets, postRestoreCredential.plaintext)
	postRestoreWriteEventID := "oci-after-restore-write-" + fixture.suffix
	releaseOCIIngestEvent(
		t,
		ctx,
		stack,
		fixture.restoredGRPCAddress,
		postRestoreCredential,
		postRestoreIndex,
		postRestoreWriteEventID,
		"paired recovery post-restore control and ingest write",
		fixture.fixtureTime.Add(3*time.Second),
		1,
	)
	releaseOCIAssertSearchEventIDs(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		postRestoreIndex,
		fixture.fixtureTime,
		[]string{postRestoreWriteEventID},
	)
	restoredBootstrap = releaseOCIBootstrap(
		t,
		ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
	)
	if !slices.ContainsFunc(restoredBootstrap.GetIndexes(), func(index *opensplunkv1.IndexSummary) bool {
		return index.GetName() == postRestoreIndex
	}) {
		t.Fatalf(
			"restored bootstrap indexes do not contain post-restore mutation %q: %+v",
			postRestoreIndex,
			restoredBootstrap.GetIndexes(),
		)
	}
}

type releaseOCIIngestionCredential struct {
	tokenID     string
	tokenPrefix string
	plaintext   string
	collectorID string
}

func releaseOCICreateIngestionCredential(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	name string,
	indexName string,
	collectorID string,
) releaseOCIIngestionCredential {
	t.Helper()
	var created opensplunkv1.CreateIngestionTokenResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/create",
		administratorToken,
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: name,
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{indexName},
					BoundCollectorId:  &collectorID,
				},
			},
		},
		&created,
	)
	metadata := created.GetIngestionToken()
	plaintext := created.GetPlaintextToken()
	if metadata.GetIngestionTokenId() == "" || metadata.GetVersion() != 1 ||
		metadata.GetState() != opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE ||
		metadata.GetTokenPrefix() == "" || plaintext == "" ||
		!strings.HasPrefix(plaintext, metadata.GetTokenPrefix()) ||
		metadata.GetConstraints().GetBoundCollectorId() != collectorID ||
		!slices.Equal(metadata.GetConstraints().GetAllowedIndexNames(), []string{indexName}) {
		t.Fatalf(
			"created release recovery ingestion credential metadata = %+v, plaintext length = %d",
			metadata,
			len(plaintext),
		)
	}
	return releaseOCIIngestionCredential{
		tokenID:     metadata.GetIngestionTokenId(),
		tokenPrefix: metadata.GetTokenPrefix(),
		plaintext:   plaintext,
		collectorID: collectorID,
	}
}

func releaseOCIAssertRestoredIngestionCredential(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	credential releaseOCIIngestionCredential,
	indexName string,
) {
	t.Helper()
	var response opensplunkv1.GetIngestionTokenResponse
	wire := postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/get",
		administratorToken,
		&opensplunkv1.GetIngestionTokenRequest{IngestionTokenId: credential.tokenID},
		&response,
	)
	if bytes.Contains(wire, []byte(credential.plaintext)) {
		t.Fatal("restored ingestion credential response exposed plaintext")
	}
	metadata := response.GetIngestionToken()
	if metadata.GetIngestionTokenId() != credential.tokenID ||
		metadata.GetTokenPrefix() != credential.tokenPrefix ||
		metadata.GetState() != opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE ||
		metadata.GetLastUsedAt() == nil ||
		metadata.GetConstraints().GetBoundCollectorId() != credential.collectorID ||
		!slices.Equal(metadata.GetConstraints().GetAllowedIndexNames(), []string{indexName}) {
		t.Fatalf("restored release recovery ingestion credential = %+v", metadata)
	}
}

func releaseOCIIngestEvent(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	grpcAddress string,
	credential releaseOCIIngestionCredential,
	indexName string,
	eventID string,
	message string,
	eventTime time.Time,
	batchSequence uint64,
) {
	t.Helper()
	if batchSequence == 0 {
		t.Fatal("release recovery ingestion batch sequence must be positive")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(releaseOCIReadFile(
		t,
		stack.values["OPEN_SPLUNK_SERVER_TLS_CA_FILE"],
	))) {
		t.Fatal("generated server CA contains no certificates for recovery ingestion")
	}
	connection, err := grpc.NewClient(
		grpcAddress,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: stack.values["OPEN_SPLUNK_SERVER_TLS_SERVER_NAME"],
		})),
	)
	if err != nil {
		t.Fatalf("create release recovery ingestion client: %s", stack.redact(err.Error()))
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close release recovery ingestion client: %v", closeErr)
		}
	}()
	streamContext, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	streamContext = metadata.NewOutgoingContext(
		streamContext,
		metadata.Pairs("authorization", "Bearer "+credential.plaintext),
	)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).Collect(streamContext)
	if err != nil {
		t.Fatalf("open release recovery ingestion stream: %s", stack.redact(err.Error()))
	}
	now := time.Now().UTC()
	var lastAcknowledged *uint64
	if batchSequence > 1 {
		value := batchSequence - 1
		lastAcknowledged = &value
	}
	inputID := "oci-recovery-input-" + strings.TrimPrefix(credential.collectorID, "oci-recovery-collector-")
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(now),
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: &opensplunkv1.CollectorHello{
			CollectorId:                   credential.collectorID,
			InstanceId:                    credential.collectorID + "-instance-" + strconv.FormatUint(batchSequence, 10),
			ProtocolMajor:                 1,
			ProtocolMinor:                 0,
			CollectorVersion:              "release-oci-recovery-test",
			Hostname:                      "release-oci-recovery-test",
			StartedAt:                     timestamppb.New(now.Add(-time.Minute)),
			Capabilities:                  []opensplunkv1.CollectorCapability{opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT},
			Inputs:                        []*opensplunkv1.CollectorInputRegistration{{InputId: inputID, InputType: opensplunkv1.CollectorInputType_COLLECTOR_INPUT_TYPE_FILE, IndexName: indexName}},
			LastAcknowledgedBatchSequence: lastAcknowledged,
		}},
	}); err != nil {
		t.Fatalf("send release recovery collector hello: %s", stack.redact(err.Error()))
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive release recovery collector ready: %s", stack.redact(err.Error()))
	}
	ready := readyResponse.GetReady()
	if ready == nil || ready.GetProtocolMajor() != 1 || ready.GetProtocolMinor() != 0 ||
		!slices.Contains(ready.GetAuthorizedIndexes(), indexName) {
		t.Fatalf("release recovery collector ready = %+v", ready)
	}
	event := &opensplunkv1.LogEvent{
		EventId:         eventID,
		IndexName:       indexName,
		EventTime:       timestamppb.New(eventTime.UTC()),
		CollectedAt:     timestamppb.New(eventTime.UTC().Add(100 * time.Millisecond)),
		EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:            "release-oci-recovery-host",
		Source:          "/var/log/release-oci-recovery.log",
		Sourcetype:      "json",
		Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
		Message:         &message,
		Raw:             []byte(`{"message":"` + message + `"}`),
		RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Fields:          &opensplunkv1.TypedObject{},
	}
	batch := &opensplunkv1.EventBatch{
		CollectorId:           credential.collectorID,
		BatchId:               "oci-recovery-batch-" + eventID,
		BatchSequence:         batchSequence,
		CreatedAt:             timestamppb.New(now),
		Events:                []*opensplunkv1.LogEvent{event},
		UncompressedSizeBytes: uint64(proto.Size(event)),
		EventIdsSha256:        wal.ComputeEventIDsDigest([]*opensplunkv1.LogEvent{event}),
		ProtocolMajor:         1,
		ProtocolMinor:         0,
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(time.Now().UTC()),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: batch},
	}); err != nil {
		t.Fatalf("send release recovery event batch: %s", stack.redact(err.Error()))
	}
	for {
		response, receiveErr := stream.Recv()
		if receiveErr != nil {
			t.Fatalf("receive release recovery event acknowledgment: %s", stack.redact(receiveErr.Error()))
		}
		if rejection := response.GetBatchReject(); rejection != nil {
			t.Fatalf("release recovery event batch rejected: %+v", rejection)
		}
		if retry := response.GetRetryBatch(); retry != nil {
			t.Fatalf("release recovery event batch unexpectedly requested retry: %+v", retry)
		}
		acknowledgment := response.GetBatchAck()
		if acknowledgment == nil {
			continue
		}
		if acknowledgment.GetBatchId() != batch.GetBatchId() ||
			acknowledgment.GetBatchSequence() != batchSequence ||
			acknowledgment.GetDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED ||
			acknowledgment.GetAcceptedEventCount() != 1 ||
			acknowledgment.GetDuplicateEventCount() != 0 ||
			len(acknowledgment.GetRejectedEvents()) != 0 {
			t.Fatalf("release recovery event acknowledgment = %+v", acknowledgment)
		}
		break
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 3,
		SentAt:         timestamppb.New(time.Now().UTC()),
		Payload: &opensplunkv1.CollectRequest_Goodbye{Goodbye: &opensplunkv1.CollectorGoodbye{
			Reason: opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		t.Fatalf("send release recovery collector goodbye: %s", stack.redact(err.Error()))
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		if err == nil {
			t.Fatal("release recovery collector goodbye received an unexpected response")
		}
		t.Fatalf("complete release recovery collector goodbye: %s", stack.redact(err.Error()))
	}
}

func releaseOCIAssertSearchEventIDs(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	indexName string,
	fixtureTime time.Time,
	want []string,
) {
	t.Helper()
	earliest := fixtureTime.Add(-time.Minute).Format(time.RFC3339Nano)
	latest := fixtureTime.Add(time.Hour).Format(time.RFC3339Nano)
	timezone := "UTC"
	var created opensplunkv1.CreateSearchJobResponse
	postProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/search/jobs/create",
		&opensplunkv1.CreateSearchJobRequest{Definition: &opensplunkv1.SearchDefinition{
			Spl: "index=" + indexName + " | dedup event_id | table event_id",
			TimeRange: &opensplunkv1.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			IndexScope: []string{indexName},
		}},
		&created,
	)
	jobID := created.GetSearchJob().GetSearchJobId()
	if jobID == "" {
		t.Fatalf("created release recovery search job = %+v", created.GetSearchJob())
	}
	completed := waitForCompletedSearch(t, ctx, client, baseURL, jobID, 45*time.Second)
	if completed.GetIndexTimeCutoff() == nil || completed.GetResultsTruncated() {
		t.Fatalf("completed release recovery search = %+v", completed)
	}
	pageSize := uint32(100)
	var results opensplunkv1.GetSearchResultsResponse
	postProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/search/jobs/results",
		&opensplunkv1.GetSearchResultsRequest{
			SearchJobId: jobID,
			Page: &opensplunkv1.PageRequest{
				PageSize:         &pageSize,
				IncludeTotalSize: true,
			},
		},
		&results,
	)
	page := results.GetResultPage()
	if results.GetSearchJobId() != jobID || page.GetSchema() == nil || page.GetPage() == nil ||
		!page.GetSnapshotComplete() || !page.GetPage().GetTotalSizeExact() ||
		page.GetPage().GetNextPageToken() != "" || page.GetPage().GetTotalSize() != uint64(len(want)) {
		t.Fatalf("release recovery search result page = %+v", page)
	}
	columnIndex := -1
	for index, column := range page.GetSchema().GetColumns() {
		if column.GetFieldName() == "event_id" {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		t.Fatalf("release recovery search schema lacks event_id: %+v", page.GetSchema())
	}
	got := make([]string, 0, len(page.GetRows()))
	for _, row := range page.GetRows() {
		if columnIndex >= len(row.GetCells()) {
			t.Fatalf("release recovery search row does not match schema: %+v", row)
		}
		value := row.GetCells()[columnIndex]
		if _, ok := value.GetKind().(*opensplunkv1.TypedValue_StringValue); !ok || value.GetStringValue() == "" {
			t.Fatalf("release recovery event_id cell = %+v", value)
		}
		got = append(got, value.GetStringValue())
	}
	want = slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("release recovery search event IDs = %v, want %v", got, want)
	}
}

func releaseOCIVolumeAt(
	t *testing.T,
	container *releaseOCIContainerInspect,
	destination string,
) string {
	t.Helper()
	for _, mount := range container.Mounts {
		if mount.Destination == destination {
			if mount.Type != "volume" || mount.Name == "" || !mount.RW {
				t.Fatalf("container mount at %q = %+v, want writable named volume", destination, mount)
			}
			return mount.Name
		}
	}
	t.Fatalf("container has no volume at %q", destination)
	return ""
}

func releaseOCIReadOnlyVolumeAt(
	t *testing.T,
	container *releaseOCIContainerInspect,
	destination string,
) string {
	t.Helper()
	for _, mount := range container.Mounts {
		if mount.Destination == destination {
			if mount.Type != "volume" || mount.Name == "" || mount.RW {
				t.Fatalf("container mount at %q = %+v, want read-only named volume", destination, mount)
			}
			return mount.Name
		}
	}
	t.Fatalf("container has no volume at %q", destination)
	return ""
}

func releaseOCICreateRecoveryVolume(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	purpose string,
) string {
	t.Helper()
	name := stack.project + "-" + purpose
	stack.ownedVolumes = append(stack.ownedVolumes, name)
	created := releaseOCIRunDocker(
		t,
		ctx,
		stack,
		"create "+purpose+" volume",
		"volume",
		"create",
		"--label",
		"com.open-splunk.integration.project="+stack.project,
		name,
	)
	if created != name {
		t.Fatalf("created %s volume = %q, want %q", purpose, created, name)
	}
	return name
}

func releaseOCIQueryClickHouseBootstrap(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	operation string,
	query string,
) string {
	t.Helper()
	return stack.mustCompose(
		t,
		ctx,
		operation,
		"exec",
		"--no-TTY",
		"clickhouse",
		"clickhouse-client",
		"--config-file",
		"/etc/clickhouse-client/open-splunk-tls.xml",
		"--secure",
		"--host",
		"127.0.0.1",
		"--port",
		"9440",
		"--tls-sni-override",
		stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		"--user",
		"open_splunk_bootstrap",
		"--query",
		query,
	)
}

func releaseOCIAssertClickHouseArchiveMarkerAbsent(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	label string,
) {
	t.Helper()
	const query = `
		SELECT count()
		FROM open_splunk.recovery_archive_markers
		FORMAT TSVRaw`
	if got := releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"inspect "+label+" recovery archive marker",
		query,
	); got != "0" {
		t.Fatalf("%s recovery archive marker count = %q, want 0", label, got)
	}
}

func releaseOCIAssertClickHouseRecoveryNamespace(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	label string,
	want string,
) {
	t.Helper()
	const query = `
		SELECT arrayStringConcat(arraySort(groupArray(name)), ',')
		FROM system.databases
		WHERE startsWith(name, 'open_splunk')
		FORMAT TSVRaw`
	if got := releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"inspect "+label+" recovery database namespace",
		query,
	); got != want {
		t.Fatalf("%s recovery database namespace = %q, want %q", label, got, want)
	}
}

func releaseOCIAssertRestoredClickHouseRecoveryIdentity(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	label string,
) {
	t.Helper()
	releaseOCIAssertClickHouseArchiveMarkerAbsent(t, ctx, stack, label)
	releaseOCIAssertClickHouseRecoveryNamespace(t, ctx, stack, label, "open_splunk")
	const receiptQuery = `
		SELECT count(), toString(any(recovery_archive_markers_table_uuid))
		FROM open_splunk.recovery_sets
		WHERE slot = 1
		FORMAT TSVRaw`
	receipt := releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"inspect "+label+" recovery receipt",
		receiptQuery,
	)
	receiptFields := strings.Split(receipt, "\t")
	if len(receiptFields) != 2 || receiptFields[0] != "1" {
		t.Fatalf("%s recovery receipt identity = %q, want one UUID-bound row", label, receipt)
	}
	const tableQuery = `
		SELECT toString(uuid)
		FROM system.tables
		WHERE database = 'open_splunk' AND name = 'recovery_archive_markers'
		FORMAT TSVRaw`
	tableUUID := releaseOCIQueryClickHouseBootstrap(
		t,
		ctx,
		stack,
		"inspect "+label+" recovery archive marker table UUID",
		tableQuery,
	)
	if tableUUID == "" || tableUUID == "00000000-0000-0000-0000-000000000000" ||
		receiptFields[1] != tableUUID {
		t.Fatalf(
			"%s receipt marker-table UUID = %q, physical UUID = %q",
			label,
			receiptFields[1],
			tableUUID,
		)
	}
}

func releaseOCIAssertDefaultClickHouseUserRejected(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	output, truncated, err := stack.runCompose(
		ctx,
		"exec",
		"--no-TTY",
		"--env",
		"CLICKHOUSE_PASSWORD=",
		"clickhouse",
		"clickhouse-client",
		"--host",
		"127.0.0.1",
		"--user",
		"default",
		"--query",
		"SELECT 1",
	)
	if truncated {
		t.Fatalf(
			"base ClickHouse default-user probe produced excessive output: %s",
			stack.redact(formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes)),
		)
	}
	if err == nil {
		t.Fatal("base ClickHouse default user unexpectedly connected without a password")
	}
	lowerOutput := strings.ToLower(output)
	if !strings.Contains(lowerOutput, "authentication failed") &&
		!strings.Contains(lowerOutput, "password is incorrect") {
		t.Fatalf(
			"base ClickHouse default-user rejection = %v: %s, want authentication failure",
			err,
			stack.redact(output),
		)
	}
}

type releaseOCIComposeStack struct {
	project          string
	repository       string
	deployDirectory  string
	composeFile      string
	composeOverrides []string
	envFile          string
	values           map[string]string
	retainedSecrets  []string
	serverImage      string
	collectorImage   string
	ownedContainers  []string
	ownedVolumes     []string
}

func (stack *releaseOCIComposeStack) forgetOwnedContainer(name string) {
	for index, candidate := range stack.ownedContainers {
		if candidate == name {
			stack.ownedContainers = slices.Delete(stack.ownedContainers, index, index+1)
			return
		}
	}
}

func (stack *releaseOCIComposeStack) environment() []string {
	replaced := make(map[string]struct{}, len(stack.values))
	for key := range stack.values {
		replaced[key] = struct{}{}
	}
	environment := make([]string, 0, len(os.Environ())+len(stack.values))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, exists := replaced[key]; !exists {
			environment = append(environment, entry)
		}
	}
	for key, value := range stack.values {
		environment = append(environment, key+"="+value)
	}
	return environment
}

func (stack *releaseOCIComposeStack) composeCommand(
	ctx context.Context,
	arguments ...string,
) *exec.Cmd {
	composeArguments := []string{
		"compose",
		"--env-file",
		stack.envFile,
		"--project-name",
		stack.project,
		"--project-directory",
		stack.deployDirectory,
		"--file",
		stack.composeFile,
	}
	for _, override := range stack.composeOverrides {
		composeArguments = append(composeArguments, "--file", override)
	}
	composeArguments = append(composeArguments, arguments...)
	command := exec.CommandContext(ctx, "docker", composeArguments...)
	command.Dir = stack.deployDirectory
	command.Env = stack.environment()
	configureProcessGroup(command)
	return command
}

func (stack *releaseOCIComposeStack) runCompose(
	ctx context.Context,
	arguments ...string,
) (string, bool, error) {
	return runCommandWithBoundedOutput(
		stack.composeCommand(ctx, arguments...),
		maximumHarnessOutputBytes,
	)
}

func (stack *releaseOCIComposeStack) mustCompose(
	t *testing.T,
	ctx context.Context,
	operation string,
	arguments ...string,
) string {
	t.Helper()
	output, truncated, err := stack.runCompose(ctx, arguments...)
	if err != nil || truncated {
		logs, _, _ := stack.runCompose(ctx, "logs", "--no-color", "--tail", "200")
		t.Fatalf(
			"%s: %v\noutput:\n%s\nlogs:\n%s",
			operation,
			err,
			stack.redact(formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes)),
			stack.redact(logs),
		)
	}
	return strings.TrimSpace(output)
}

func (stack *releaseOCIComposeStack) serviceContainerID(
	t *testing.T,
	ctx context.Context,
	service string,
	includeStopped bool,
) string {
	t.Helper()
	arguments := []string{"ps", "--quiet"}
	if includeStopped {
		arguments = append(arguments, "--all")
	}
	arguments = append(arguments, service)
	id := strings.TrimSpace(stack.mustCompose(t, ctx, "resolve "+service+" container", arguments...))
	if id == "" || strings.ContainsAny(id, " \t\r\n") {
		t.Fatalf("resolved %s container ID = %q", service, id)
	}
	return id
}

func (stack *releaseOCIComposeStack) secrets() []string {
	result := append([]string(nil), stack.retainedSecrets...)
	result = append(result,
		stack.values["OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD"],
		stack.values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD"],
		stack.values["OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD"],
		stack.values["OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD"],
		stack.values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD"],
		stack.values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"],
	)
	if token, err := os.ReadFile(stack.values["OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"]); err == nil {
		result = append(result, strings.TrimSpace(string(token)))
	}
	return result
}

func (stack *releaseOCIComposeStack) redact(value string) string {
	for _, secret := range stack.secrets() {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

func (stack *releaseOCIComposeStack) cleanup(t *testing.T) {
	t.Helper()
	cleanupContext, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	if output, truncated, err := stack.runCompose(
		cleanupContext,
		"down",
		"--volumes",
		"--remove-orphans",
		"--timeout",
		"10",
	); err != nil || truncated {
		t.Errorf(
			"clean release Compose project %q: %v: %s",
			stack.project,
			err,
			stack.redact(formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes)),
		)
	}
	cancel()
	for _, container := range stack.ownedContainers {
		containerContext, containerCancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(
			containerContext,
			"docker",
			"container",
			"rm",
			"--force",
			"--volumes",
			container,
		)
		configureProcessGroup(command)
		output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
		containerCancel()
		if err != nil && !strings.Contains(strings.ToLower(output), "no such container") {
			t.Errorf(
				"remove release OCI test container %q: %v: %s",
				container,
				err,
				formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
			)
		}
		if truncated {
			t.Errorf(
				"remove release OCI test container %q produced excessive output: %s",
				container,
				formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes),
			)
		}
	}
	stack.cleanupLabeledResources(t, "container")
	for _, volume := range stack.ownedVolumes {
		volumeContext, volumeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(volumeContext, "docker", "volume", "rm", "--force", volume)
		configureProcessGroup(command)
		output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
		volumeCancel()
		if err != nil && !strings.Contains(strings.ToLower(output), "no such volume") {
			t.Errorf(
				"remove release OCI test volume %q: %v: %s",
				volume,
				err,
				formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
			)
		}
		if truncated {
			t.Errorf(
				"remove release OCI test volume %q produced excessive output: %s",
				volume,
				formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes),
			)
		}
	}
	stack.cleanupLabeledResources(t, "volume")
	stack.cleanupLabeledResources(t, "network")
	for _, image := range []string{stack.serverImage, stack.collectorImage} {
		imageContext, imageCancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(imageContext, "docker", "image", "rm", "--force", image)
		configureProcessGroup(command)
		output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
		imageCancel()
		if err != nil && !strings.Contains(output, "No such image") {
			t.Errorf(
				"remove release OCI test image %q: %v: %s",
				image,
				err,
				formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
			)
		}
		if truncated {
			t.Errorf(
				"remove release OCI test image %q produced excessive output: %s",
				image,
				formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes),
			)
		}
	}
	stack.assertImagesRemoved(t)
	for _, kind := range []string{"container", "volume", "network"} {
		if leftovers := stack.labeledResources(t, kind); len(leftovers) != 0 {
			t.Errorf(
				"release OCI test project %q retained %s resources: %v",
				stack.project,
				kind,
				leftovers,
			)
		}
	}
}

func (stack *releaseOCIComposeStack) assertImagesRemoved(t *testing.T) {
	t.Helper()
	for _, image := range []string{stack.serverImage, stack.collectorImage} {
		inspectContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		command := exec.CommandContext(inspectContext, "docker", "image", "inspect", image)
		configureProcessGroup(command)
		output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
		cancel()
		if err == nil || !strings.Contains(strings.ToLower(output), "no such image") {
			t.Errorf(
				"release OCI test image %q remains after cleanup or could not be inventoried: %v: %s",
				image,
				err,
				formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
			)
		}
		if truncated {
			t.Errorf(
				"inventory removed release OCI test image %q produced excessive output: %s",
				image,
				formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes),
			)
		}
	}
}

func (stack *releaseOCIComposeStack) cleanupLabeledResources(t *testing.T, kind string) {
	t.Helper()
	resources := stack.labeledResources(t, kind)
	if len(resources) == 0 {
		return
	}
	arguments := []string{kind, "rm"}
	switch kind {
	case "container":
		arguments = append(arguments, "--force", "--volumes")
	case "volume":
		arguments = append(arguments, "--force")
	case "network":
	default:
		t.Errorf("clean release OCI project %q: unsupported Docker resource kind %q", stack.project, kind)
		return
	}
	arguments = append(arguments, resources...)
	cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	command := exec.CommandContext(cleanupContext, "docker", arguments...)
	configureProcessGroup(command)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	cancel()
	if err != nil && !strings.Contains(strings.ToLower(output), "no such") {
		t.Errorf(
			"remove labeled release OCI test %s resources %v: %v: %s",
			kind,
			resources,
			err,
			formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
		)
	}
	if truncated {
		t.Errorf(
			"remove labeled release OCI test %s resources %v produced excessive output: %s",
			kind,
			resources,
			formatBoundedCommandOutput(output, true, maximumHarnessOutputBytes),
		)
	}
}

func (stack *releaseOCIComposeStack) labeledResources(t *testing.T, kind string) []string {
	t.Helper()
	resourceSet := make(map[string]struct{})
	for _, label := range []string{
		"com.docker.compose.project=" + stack.project,
		"com.open-splunk.integration.project=" + stack.project,
	} {
		arguments := []string{kind, "ls"}
		if kind == "container" {
			arguments = append(arguments, "--all")
		}
		arguments = append(arguments, "--quiet", "--filter", "label="+label)
		listContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		command := exec.CommandContext(listContext, "docker", arguments...)
		configureProcessGroup(command)
		output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
		cancel()
		if err != nil || truncated {
			t.Errorf(
				"inventory release OCI test %s resources for label %q: %v: %s",
				kind,
				label,
				err,
				formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
			)
			continue
		}
		for resource := range strings.FieldsSeq(output) {
			resourceSet[resource] = struct{}{}
		}
	}
	resources := make([]string, 0, len(resourceSet))
	for resource := range resourceSet {
		resources = append(resources, resource)
	}
	slices.Sort(resources)
	return resources
}

func releaseOCIRequireCompose(t *testing.T, ctx context.Context, repository string) {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", "compose", "version")
	command.Dir = repository
	configureProcessGroup(command)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if err != nil || truncated {
		t.Fatalf(
			"Docker Compose is required when %s=1: %v: %s",
			releaseOCIIntegrationFlag,
			err,
			formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
		)
	}
}

func releaseOCIRequirePinnedClickHouse(t *testing.T) {
	t.Helper()
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if image != testsupport.DefaultClickHouseImage {
		t.Fatalf(
			"release OCI integration requires exact ClickHouse image %q, got %q",
			testsupport.DefaultClickHouseImage,
			image,
		)
	}
}

func releaseOCIGenerateEnvironment(
	t *testing.T,
	ctx context.Context,
	deployDirectory string,
	envFile string,
) map[string]string {
	t.Helper()
	command := exec.CommandContext(ctx, filepath.Join(deployDirectory, "generate-env.sh"), envFile)
	command.Dir = deployDirectory
	configureProcessGroup(command)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if err != nil || truncated {
		t.Fatalf(
			"generate release deployment environment: %v: %s",
			err,
			formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes),
		)
	}
	contents := releaseOCIReadFile(t, envFile)
	values := make(map[string]string)
	for lineNumber, line := range strings.Split(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			t.Fatalf("generated env line %d is invalid", lineNumber+1)
		}
		if strings.HasPrefix(value, `"`) {
			decoded, decodeErr := strconv.Unquote(value)
			if decodeErr != nil {
				t.Fatalf("decode generated env line %d: %v", lineNumber+1, decodeErr)
			}
			value = decoded
		}
		if _, duplicate := values[key]; duplicate {
			t.Fatalf("generated env contains duplicate key %q", key)
		}
		values[key] = value
	}
	for _, key := range []string{
		"OPEN_SPLUNK_APPLICATION_VERSION",
		"OPEN_SPLUNK_SOURCE_REVISION",
		"OPEN_SPLUNK_IMAGE_CREATED",
		"OPEN_SPLUNK_SOURCE_DATE_EPOCH",
		"OPEN_SPLUNK_SERVER_IMAGE",
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME",
		"OPEN_SPLUNK_SERVER_TLS_CA_FILE",
		"OPEN_SPLUNK_SERVER_TLS_CERT_FILE",
		"OPEN_SPLUNK_SERVER_TLS_KEY_FILE",
		"OPEN_SPLUNK_SERVER_TLS_SERVER_NAME",
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE",
	} {
		if values[key] == "" {
			t.Fatalf("generated environment is missing %s", key)
		}
	}
	return values
}

func releaseOCIBuildImages(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	command := exec.CommandContext(ctx, "make", "oci")
	command.Dir = stack.repository
	command.Env = stack.environment()
	configureProcessGroup(command)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	// Docker Desktop BuildKit may leave an inherited progress pipe open after
	// make and both docker build commands have exited successfully. Go reports
	// that otherwise-successful exit as ErrWaitDelay after closing the orphaned
	// pipe. The immediately following image inspection is the authoritative
	// proof that both requested images were published.
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil
	}
	if err != nil || truncated {
		t.Fatalf(
			"build exact release OCI images through make oci: %v: %s",
			err,
			stack.redact(formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes)),
		)
	}
}

type releaseOCIImageInspect struct {
	Architecture string `json:"Architecture"`
	OS           string `json:"Os"`
	Config       struct {
		User       string            `json:"User"`
		Entrypoint []string          `json:"Entrypoint"`
		Cmd        []string          `json:"Cmd"`
		Env        []string          `json:"Env"`
		Labels     map[string]string `json:"Labels"`
	} `json:"Config"`
}

type releaseOCIComposeVolume struct {
	Type     string `json:"type"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}

func releaseOCIAssertImageContract(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	serverImage string,
	collectorImage string,
) {
	t.Helper()
	server := releaseOCIInspectImage(t, ctx, stack, serverImage)
	collector := releaseOCIInspectImage(t, ctx, stack, collectorImage)
	for name, image := range map[string]releaseOCIImageInspect{
		"server":    server,
		"collector": collector,
	} {
		if image.OS != "linux" || image.Architecture != runtime.GOARCH {
			t.Fatalf("%s image platform = %s/%s, want linux/%s", name, image.OS, image.Architecture, runtime.GOARCH)
		}
		if image.Config.User != "65532:65532" {
			t.Fatalf("%s image user = %q", name, image.Config.User)
		}
		for label, want := range map[string]string{
			"org.opencontainers.image.version":  stack.values["OPEN_SPLUNK_APPLICATION_VERSION"],
			"org.opencontainers.image.revision": stack.values["OPEN_SPLUNK_SOURCE_REVISION"],
			"org.opencontainers.image.created":  stack.values["OPEN_SPLUNK_IMAGE_CREATED"],
			"org.opencontainers.image.source":   "https://github.com/Suhaibinator/open-splunk",
			"org.opencontainers.image.licenses": "MIT",
		} {
			if image.Config.Labels[label] != want {
				t.Fatalf("%s image label %s = %q, want %q", name, label, image.Config.Labels[label], want)
			}
		}
		for _, entry := range image.Config.Env {
			if entry != "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" {
				t.Fatalf("%s scratch image contains unexpected environment default %q", name, entry)
			}
		}
	}
	if !slices.Equal(server.Config.Entrypoint, []string{"/usr/local/bin/open-splunk-server"}) || len(server.Config.Cmd) != 0 {
		t.Fatalf("server image process = entrypoint %v cmd %v", server.Config.Entrypoint, server.Config.Cmd)
	}
	if !slices.Equal(collector.Config.Entrypoint, []string{"/usr/local/bin/open-splunk-collector"}) ||
		!slices.Equal(collector.Config.Cmd, []string{"run", "-config", "/etc/open-splunk/collector.yaml"}) {
		t.Fatalf("collector image process = entrypoint %v cmd %v", collector.Config.Entrypoint, collector.Config.Cmd)
	}

	serverIdentity := releaseOCIRunImageIdentity(
		t, ctx, stack, serverImage,
		"/usr/local/bin/open-splunk-server", "-verify-embedded-release",
	)
	collectorIdentity := releaseOCIRunImageIdentity(
		t, ctx, stack, collectorImage,
		"/usr/local/bin/open-splunk-collector", "version",
	)
	for name, identity := range map[string]string{
		"server":    serverIdentity,
		"collector": collectorIdentity,
	} {
		for _, expected := range []string{
			"application_version=" + stack.values["OPEN_SPLUNK_APPLICATION_VERSION"],
			"source_revision=" + stack.values["OPEN_SPLUNK_SOURCE_REVISION"],
		} {
			if !strings.Contains(identity, expected+"\n") && !strings.HasSuffix(identity, expected) {
				t.Fatalf("%s image identity %q does not contain %q", name, identity, expected)
			}
		}
	}
	expectedSPLIdentity := "spl_compatibility_version=" + spl.CompatibilityVersion
	if !strings.Contains(serverIdentity, expectedSPLIdentity+"\n") &&
		!strings.HasSuffix(serverIdentity, expectedSPLIdentity) {
		t.Fatalf("server image identity %q does not contain %q", serverIdentity, expectedSPLIdentity)
	}
}

func releaseOCIInspectImage(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	image string,
) releaseOCIImageInspect {
	t.Helper()
	output := releaseOCIRunDocker(t, ctx, stack, "inspect image", "image", "inspect", image)
	var decoded []releaseOCIImageInspect
	if err := json.Unmarshal([]byte(output), &decoded); err != nil || len(decoded) != 1 {
		t.Fatalf("decode image inspect for %s: %v", image, err)
	}
	return decoded[0]
}

func releaseOCIRunImageIdentity(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	image string,
	entrypoint string,
	arguments ...string,
) string {
	t.Helper()
	dockerArguments := []string{
		"run", "--rm",
		"--network", "none",
		"--read-only",
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges:true",
		"--entrypoint", entrypoint,
		image,
	}
	dockerArguments = append(dockerArguments, arguments...)
	return releaseOCIRunDocker(t, ctx, stack, "run image identity", dockerArguments...)
}

func releaseOCIAssertProductionComposeConfig(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	output := stack.mustCompose(t, ctx, "render production Compose config", "config", "--format", "json")
	type composePort struct {
		HostIP    string `json:"host_ip"`
		Published string `json:"published"`
		Protocol  string `json:"protocol"`
		Target    uint16 `json:"target"`
	}
	var config struct {
		Services map[string]struct {
			Image   string                    `json:"image"`
			Ports   []composePort             `json:"ports"`
			Build   json.RawMessage           `json:"build"`
			Volumes []releaseOCIComposeVolume `json:"volumes"`
		} `json:"services"`
	}
	if err := json.Unmarshal([]byte(output), &config); err != nil {
		t.Fatalf("decode production Compose config: %v", err)
	}
	wantServices := []string{
		"clickhouse",
		"clickhouse-migrator",
		"clickhouse-recovery-volume-bootstrap",
		"server",
		"server-bootstrap",
	}
	if got := mapsKeys(config.Services); !slices.Equal(got, wantServices) {
		t.Fatalf("production Compose services = %v, want %v", got, wantServices)
	}
	if _, exists := config.Services["collector"]; exists {
		t.Fatal("collector unexpectedly appears in the default production Compose deployment")
	}
	for name, service := range config.Services {
		build := bytes.TrimSpace(service.Build)
		if len(build) != 0 && !bytes.Equal(build, []byte("null")) {
			t.Fatalf("production Compose service %q has a source build config: %s", name, build)
		}
	}
	if config.Services["server"].Image != stack.serverImage ||
		config.Services["clickhouse-migrator"].Image != stack.serverImage ||
		config.Services["clickhouse-recovery-volume-bootstrap"].Image != stack.serverImage ||
		config.Services["server-bootstrap"].Image != stack.serverImage {
		t.Fatalf("production server images do not use %q", stack.serverImage)
	}
	releaseOCIAssertComposeRecoveryMount(
		t,
		"normal ClickHouse",
		config.Services["clickhouse"].Volumes,
		false,
	)
	server := config.Services["server"]
	wantServerPorts := map[uint16]bool{8080: false, 4317: false}
	if len(server.Ports) != len(wantServerPorts) {
		t.Fatalf(
			"production server ports = %+v, want exactly loopback TCP targets 8080 and 4317",
			server.Ports,
		)
	}
	for _, port := range server.Ports {
		if _, expected := wantServerPorts[port.Target]; !expected || wantServerPorts[port.Target] {
			t.Fatalf(
				"production server ports = %+v, want unique TCP targets 8080 and 4317",
				server.Ports,
			)
		}
		if port.HostIP != "127.0.0.1" || port.Published != "0" || port.Protocol != "tcp" {
			t.Fatalf(
				"production server port %+v, want Docker-assigned loopback TCP publication",
				port,
			)
		}
		wantServerPorts[port.Target] = true
	}
	bootstrap := config.Services["server-bootstrap"]
	if len(bootstrap.Ports) != 0 {
		t.Fatalf("production server bootstrap publishes host ports: %+v", bootstrap.Ports)
	}
	migrator := config.Services["clickhouse-migrator"]
	if len(migrator.Ports) != 0 {
		t.Fatalf("production ClickHouse migrator publishes host ports: %+v", migrator.Ports)
	}
	recoveryVolumeBootstrap := config.Services["clickhouse-recovery-volume-bootstrap"]
	if len(recoveryVolumeBootstrap.Ports) != 0 {
		t.Fatalf(
			"production ClickHouse recovery volume bootstrap publishes host ports: %+v",
			recoveryVolumeBootstrap.Ports,
		)
	}
	clickHouse := config.Services["clickhouse"]
	if clickHouse.Image != testsupport.DefaultClickHouseImage {
		t.Fatalf("production ClickHouse image = %q, want %q", clickHouse.Image, testsupport.DefaultClickHouseImage)
	}
	if len(clickHouse.Ports) != 0 {
		t.Fatalf("production ClickHouse publishes host ports: %+v", clickHouse.Ports)
	}
}

func releaseOCIAssertRestoreComposeConfig(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	output := stack.mustCompose(
		t,
		ctx,
		"render restore Compose config",
		"--profile",
		"recovery",
		"config",
		"--format",
		"json",
	)
	var config struct {
		Services map[string]struct {
			Volumes []releaseOCIComposeVolume `json:"volumes"`
		} `json:"services"`
	}
	if err := json.Unmarshal([]byte(output), &config); err != nil {
		t.Fatalf("decode restore Compose config: %v", err)
	}
	for _, service := range []string{"clickhouse", "deployment-restore"} {
		value, exists := config.Services[service]
		if !exists {
			t.Fatalf("restore Compose config has no %q service", service)
		}
		releaseOCIAssertComposeRecoveryMount(t, service, value.Volumes, true)
	}
}

func releaseOCIAssertComposeRecoveryMount(
	t *testing.T,
	label string,
	volumes []releaseOCIComposeVolume,
	wantReadOnly bool,
) {
	t.Helper()
	var matches []releaseOCIComposeVolume
	for _, volume := range volumes {
		if volume.Target == releaseOCIClickHouseRecoveryPath ||
			volume.Target == "/var/lib/open-splunk/clickhouse-backups" {
			matches = append(matches, volume)
		}
	}
	if len(matches) != 1 || matches[0].Type != "volume" ||
		matches[0].Source != "clickhouse-recovery" ||
		matches[0].ReadOnly != wantReadOnly {
		t.Fatalf(
			"%s recovery mounts = %+v, want one clickhouse-recovery named volume with read_only=%t",
			label,
			matches,
			wantReadOnly,
		)
	}
}

func mapsKeys[V any](input map[string]V) []string {
	result := make([]string, 0, len(input))
	for key := range input {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

type releaseOCIContainerInspect struct {
	Config struct {
		User        string   `json:"User"`
		WorkingDir  string   `json:"WorkingDir"`
		Env         []string `json:"Env"`
		Entrypoint  []string `json:"Entrypoint"`
		Cmd         []string `json:"Cmd"`
		Healthcheck *struct {
			Test []string `json:"Test"`
		} `json:"Healthcheck"`
	} `json:"Config"`
	HostConfig struct {
		ReadonlyRootfs    bool                  `json:"ReadonlyRootfs"`
		AutoRemove        bool                  `json:"AutoRemove"`
		Privileged        bool                  `json:"Privileged"`
		NetworkMode       string                `json:"NetworkMode"`
		PidMode           string                `json:"PidMode"`
		IpcMode           string                `json:"IpcMode"`
		UTSMode           string                `json:"UTSMode"`
		UsernsMode        string                `json:"UsernsMode"`
		CgroupnsMode      string                `json:"CgroupnsMode"`
		PidsLimit         int64                 `json:"PidsLimit"`
		CapAdd            []string              `json:"CapAdd"`
		CapDrop           []string              `json:"CapDrop"`
		GroupAdd          []string              `json:"GroupAdd"`
		Devices           []json.RawMessage     `json:"Devices"`
		DeviceRequests    []json.RawMessage     `json:"DeviceRequests"`
		DeviceCgroupRules []string              `json:"DeviceCgroupRules"`
		VolumesFrom       []string              `json:"VolumesFrom"`
		SecurityOpt       []string              `json:"SecurityOpt"`
		PortBindings      map[string][]struct{} `json:"PortBindings"`
		Tmpfs             map[string]string     `json:"Tmpfs"`
	} `json:"HostConfig"`
	State struct {
		Status   string `json:"Status"`
		ExitCode int    `json:"ExitCode"`
		Health   *struct {
			Status string `json:"Status"`
		} `json:"Health"`
	} `json:"State"`
	Mounts []struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	} `json:"Mounts"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct{} `json:"Networks"`
	} `json:"NetworkSettings"`
}

func releaseOCIInspectContainer(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	containerID string,
) *releaseOCIContainerInspect {
	t.Helper()
	output := releaseOCIRunDocker(t, ctx, stack, "inspect container", "inspect", containerID)
	var decoded []releaseOCIContainerInspect
	if err := json.Unmarshal([]byte(output), &decoded); err != nil || len(decoded) != 1 {
		t.Fatalf("decode container inspect for %s: %v", containerID, err)
	}
	return &decoded[0]
}

func releaseOCIAssertContainerHardening(
	t *testing.T,
	container *releaseOCIContainerInspect,
	wantUser string,
	wantRunning bool,
) {
	t.Helper()
	if container.Config.User != wantUser || !container.HostConfig.ReadonlyRootfs ||
		len(container.HostConfig.CapAdd) != 0 || !slices.Contains(container.HostConfig.CapDrop, "ALL") ||
		!slices.Contains(container.HostConfig.SecurityOpt, "no-new-privileges:true") {
		t.Fatalf(
			"container hardening = user %q readonly %t cap-add %v cap-drop %v security %v",
			container.Config.User,
			container.HostConfig.ReadonlyRootfs,
			container.HostConfig.CapAdd,
			container.HostConfig.CapDrop,
			container.HostConfig.SecurityOpt,
		)
	}
	if wantRunning && container.State.Status != "running" {
		t.Fatalf("hardened container state = %q, want running", container.State.Status)
	}
}

func releaseOCIAssertContainerHealth(
	t *testing.T,
	container *releaseOCIContainerInspect,
	name string,
) {
	t.Helper()
	if container.State.Status != "running" || container.State.Health == nil || container.State.Health.Status != "healthy" {
		t.Fatalf("%s state = status %q health %+v", name, container.State.Status, container.State.Health)
	}
}

func releaseOCIAssertBootstrapCompleted(
	t *testing.T,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	if container.State.Status != "exited" || container.State.ExitCode != 0 {
		t.Fatalf("server bootstrap state = %q exit %d", container.State.Status, container.State.ExitCode)
	}
	releaseOCIAssertOneShotHealthcheckDisabled(t, "server bootstrap", container)
}

func releaseOCIAssertRecoveryVolumeBootstrap(
	t *testing.T,
	stack *releaseOCIComposeStack,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	releaseOCIAssertOneShotHealthcheckDisabled(t, "ClickHouse recovery volume bootstrap", container)
	wantCommand := []string{
		"prepare-clickhouse-recovery-volume",
		"-path",
		"/var/lib/open-splunk/clickhouse-backups",
	}
	if container.Config.User != "0:65532" || !container.HostConfig.ReadonlyRootfs ||
		!slices.Contains(container.HostConfig.CapDrop, "ALL") ||
		!slices.Contains(container.HostConfig.SecurityOpt, "no-new-privileges:true") {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap hardening = user %q readonly %t cap-drop %v security %v",
			container.Config.User,
			container.HostConfig.ReadonlyRootfs,
			container.HostConfig.CapDrop,
			container.HostConfig.SecurityOpt,
		)
	}
	capabilities := make([]string, 0, len(container.HostConfig.CapAdd))
	for _, capability := range container.HostConfig.CapAdd {
		capabilities = append(capabilities, strings.TrimPrefix(capability, "CAP_"))
	}
	slices.Sort(capabilities)
	if !slices.Equal(capabilities, []string{"CHOWN", "FOWNER"}) {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap capabilities = %v, want CHOWN and FOWNER",
			container.HostConfig.CapAdd,
		)
	}
	if container.State.Status != "exited" || container.State.ExitCode != 0 {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap state = %q exit %d, want successful one-shot exit",
			container.State.Status,
			container.State.ExitCode,
		)
	}
	if container.HostConfig.NetworkMode != "none" {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap network mode = %q, want none",
			container.HostConfig.NetworkMode,
		)
	}
	if container.HostConfig.PidsLimit != 32 {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap PID limit = %d, want 32",
			container.HostConfig.PidsLimit,
		)
	}
	if !slices.Equal(container.Config.Cmd, wantCommand) {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap command = %q, want %q",
			container.Config.Cmd,
			wantCommand,
		)
	}
	if len(container.HostConfig.PortBindings) != 0 {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap publishes host ports: %+v",
			container.HostConfig.PortBindings,
		)
	}
	for _, entry := range container.Config.Env {
		name, value, _ := strings.Cut(entry, "=")
		if strings.Contains(name, "PASSWORD") {
			t.Fatalf(
				"ClickHouse recovery volume bootstrap environment contains password variable %q",
				name,
			)
		}
		for _, secret := range stack.secrets() {
			if secret != "" && strings.Contains(value, secret) {
				t.Fatalf(
					"ClickHouse recovery volume bootstrap environment variable %q contains secret material",
					name,
				)
			}
		}
	}
	if len(container.Mounts) != 1 {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap mounts = %+v, want one recovery volume",
			container.Mounts,
		)
	}
	mount := container.Mounts[0]
	if mount.Type != "volume" || mount.Name != stack.project+"_clickhouse-recovery" ||
		mount.Destination != "/var/lib/open-splunk/clickhouse-backups" || !mount.RW {
		t.Fatalf(
			"ClickHouse recovery volume bootstrap mount = %+v, want writable project recovery volume",
			mount,
		)
	}
}

func releaseOCIAssertClickHouseRecoveryVolumeOwnership(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	ownership := stack.mustCompose(
		t,
		ctx,
		"inspect ClickHouse recovery volume ownership",
		"exec",
		"--no-TTY",
		"clickhouse",
		"stat",
		"--format=%u:%g:%a",
		"/var/lib/open-splunk-clickhouse-backups",
	)
	if ownership != "101:65532:2750" {
		t.Fatalf(
			"ClickHouse recovery volume ownership and mode = %q, want 101:65532:2750",
			ownership,
		)
	}
}

func releaseOCIAssertMigratorCompleted(
	t *testing.T,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	if container.State.Status != "exited" || container.State.ExitCode != 0 {
		t.Fatalf(
			"ClickHouse migrator state = %q exit %d, want successful one-shot exit",
			container.State.Status,
			container.State.ExitCode,
		)
	}
	releaseOCIAssertOneShotHealthcheckDisabled(t, "ClickHouse migrator", container)
}

func releaseOCIAssertOneShotHealthcheckDisabled(
	t *testing.T,
	label string,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	if container.Config.Healthcheck != nil &&
		!slices.Equal(container.Config.Healthcheck.Test, []string{"NONE"}) {
		t.Fatalf(
			"%s healthcheck = %q, want absent or disabled",
			label,
			container.Config.Healthcheck.Test,
		)
	}
}

func releaseOCIAssertMigratorIsolation(
	t *testing.T,
	stack *releaseOCIComposeStack,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	wantCommand := []string{
		"migrate-clickhouse",
		"-address",
		"clickhouse:9440",
		"-password-file",
		"/run/open-splunk/clickhouse/migration.password",
		"-ca-cert",
		"/run/open-splunk/clickhouse/ca.crt",
		"-server-name",
		stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
	}
	if !slices.Equal(container.Config.Cmd, wantCommand) {
		t.Fatalf("ClickHouse migrator command = %q, want %q", container.Config.Cmd, wantCommand)
	}
	for _, entry := range container.Config.Env {
		name, value, _ := strings.Cut(entry, "=")
		if strings.Contains(name, "PASSWORD") {
			t.Fatalf("ClickHouse migrator environment contains password variable %q", name)
		}
		for _, secret := range stack.secrets() {
			if secret != "" && strings.Contains(value, secret) {
				t.Fatalf("ClickHouse migrator environment variable %q contains secret material", name)
			}
		}
	}
	wantMounts := map[string]string{
		"/run/open-splunk/clickhouse/ca.crt":             stack.values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"],
		"/run/open-splunk/clickhouse/migration.password": stack.values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE"],
	}
	if len(container.Mounts) != len(wantMounts) {
		t.Fatalf("ClickHouse migrator mounts = %+v, want only CA and migration secret", container.Mounts)
	}
	for _, mount := range container.Mounts {
		wantSource, expected := wantMounts[mount.Destination]
		if !expected || mount.Source != wantSource || mount.RW {
			t.Fatalf("ClickHouse migrator mount = %+v, want read-only exact input", mount)
		}
	}
	if len(container.HostConfig.PortBindings) != 0 {
		t.Fatalf("ClickHouse migrator publishes host ports: %+v", container.HostConfig.PortBindings)
	}
	if container.HostConfig.PidsLimit != 64 {
		t.Fatalf("ClickHouse migrator PID limit = %d, want 64", container.HostConfig.PidsLimit)
	}
	for port, bindings := range container.NetworkSettings.Ports {
		if len(bindings) != 0 {
			t.Fatalf("ClickHouse migrator port %s is published: %+v", port, bindings)
		}
	}
	wantNetwork := stack.project + "_backend"
	if len(container.NetworkSettings.Networks) != 1 {
		t.Fatalf(
			"ClickHouse migrator networks = %v, want only %q",
			mapsKeys(container.NetworkSettings.Networks),
			wantNetwork,
		)
	}
	if _, exists := container.NetworkSettings.Networks[wantNetwork]; !exists {
		t.Fatalf(
			"ClickHouse migrator networks = %v, want only %q",
			mapsKeys(container.NetworkSettings.Networks),
			wantNetwork,
		)
	}
}

func releaseOCIAssertBootstrapNetworkDisabled(
	t *testing.T,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	if container.HostConfig.NetworkMode != "none" {
		t.Fatalf("server bootstrap network mode = %q, want none", container.HostConfig.NetworkMode)
	}
}

func releaseOCIAssertClickHouseHasNoHostPorts(
	t *testing.T,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	if len(container.HostConfig.PortBindings) != 0 {
		t.Fatalf("production ClickHouse host port bindings = %+v", container.HostConfig.PortBindings)
	}
	for port, bindings := range container.NetworkSettings.Ports {
		if len(bindings) != 0 {
			t.Fatalf("production ClickHouse port %s is published: %+v", port, bindings)
		}
	}
}

func releaseOCIAssertServerEnvironmentHasNoSecrets(
	t *testing.T,
	stack *releaseOCIComposeStack,
	container *releaseOCIContainerInspect,
) {
	t.Helper()
	allowedPasswordFileVariables := map[string]string{
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE":  "/run/open-splunk/clickhouse/runtime.password",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE": "/run/open-splunk/clickhouse/deletion.password",
	}
	forbiddenNames := map[string]struct{}{
		"CLICKHOUSE_PASSWORD":                           {},
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD":     {},
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD":     {},
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD":       {},
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD":      {},
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD":        {},
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD":       {},
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN":               {},
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE_CONTENTS": {},
	}
	secrets := stack.secrets()
	singletonLockEnvironment := false
	for _, entry := range container.Config.Env {
		name, value, _ := strings.Cut(entry, "=")
		if name == "OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH" {
			if singletonLockEnvironment ||
				value != "/var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock" {
				t.Fatalf("server singleton-lock environment = %q", entry)
			}
			singletonLockEnvironment = true
		}
		if _, forbidden := forbiddenNames[name]; forbidden {
			t.Fatalf("server environment contains forbidden secret variable %q", name)
		}
		if strings.Contains(name, "CLICKHOUSE") && strings.Contains(name, "PASSWORD") {
			wantValue, allowed := allowedPasswordFileVariables[name]
			if !allowed {
				t.Fatalf("server environment contains unrecognized ClickHouse password variable %q", name)
			}
			if value != wantValue {
				t.Fatalf("server environment variable %q = %q, want mounted file %q", name, value, wantValue)
			}
		}
		for _, secret := range secrets {
			if secret != "" && strings.Contains(value, secret) {
				t.Fatalf("server environment variable %q contains secret material", name)
			}
		}
	}
	if !singletonLockEnvironment {
		t.Fatal("server environment omits the retained deployment singleton lock")
	}
	joinedProcess := strings.Join(append(slices.Clone(container.Config.Entrypoint), container.Config.Cmd...), "\x00")
	if !slices.Contains(container.Config.Cmd, "-clickhouse-skip-migrations") ||
		strings.Contains(joinedProcess, "migration.password") ||
		strings.Contains(joinedProcess, "backup.password") ||
		strings.Contains(joinedProcess, "restore.password") {
		t.Fatalf("server process does not isolate one-shot ClickHouse credentials: %q", joinedProcess)
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(joinedProcess, secret) {
			t.Fatal("server process arguments contain secret material")
		}
	}
	administratorSource := stack.values["OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"]
	passwordFileMounts := map[string]bool{
		"/run/open-splunk/clickhouse/runtime.password":  false,
		"/run/open-splunk/clickhouse/deletion.password": false,
	}
	singletonLockMount := false
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/lib/open-splunk/lock" {
			if singletonLockMount || mount.Type != "volume" ||
				mount.Name != stack.project+"_server-lock" || !mount.RW {
				t.Fatalf("long-running server singleton-lock mount = %+v", mount)
			}
			singletonLockMount = true
		}
		if mount.Source == administratorSource || strings.Contains(mount.Destination, "open-splunk-bootstrap") {
			t.Fatalf("long-running server mounts administrator token source: %+v", mount)
		}
		if _, required := passwordFileMounts[mount.Destination]; required {
			if mount.RW {
				t.Fatalf("server ClickHouse password file mount is writable: %+v", mount)
			}
			passwordFileMounts[mount.Destination] = true
		}
		if strings.Contains(mount.Destination, "migration.password") ||
			strings.Contains(mount.Destination, "backup.password") ||
			strings.Contains(mount.Destination, "restore.password") ||
			mount.Source == stack.values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE"] ||
			mount.Source == stack.values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE"] ||
			mount.Source == stack.values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE"] {
			t.Fatalf("long-running server mounts isolated ClickHouse credentials: %+v", mount)
		}
		if mount.Destination == "/var/lib/open-splunk/recovery" ||
			strings.Contains(mount.Destination, "clickhouse-backups") {
			t.Fatalf("long-running server mounts recovery storage: %+v", mount)
		}
	}
	if !singletonLockMount {
		t.Fatal("long-running server does not mount the retained deployment singleton lock")
	}
	for destination, mounted := range passwordFileMounts {
		if !mounted {
			t.Fatalf("server does not mount ClickHouse password file %q", destination)
		}
	}
}

func releaseOCIPublishedServerAddresses(
	t *testing.T,
	container *releaseOCIContainerInspect,
) (string, string) {
	t.Helper()
	wantPorts := []string{"8080/tcp", "4317/tcp"}
	if len(container.NetworkSettings.Ports) != len(wantPorts) {
		t.Fatalf(
			"release server published ports = %+v, want exactly %v",
			container.NetworkSettings.Ports,
			wantPorts,
		)
	}
	addresses := make([]string, len(wantPorts))
	for index, containerPort := range wantPorts {
		bindings, exists := container.NetworkSettings.Ports[containerPort]
		if !exists || len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
			t.Fatalf(
				"release server port %s bindings = %+v, want one loopback binding",
				containerPort,
				bindings,
			)
		}
		port, err := strconv.ParseUint(bindings[0].HostPort, 10, 16)
		if err != nil || port == 0 {
			t.Fatalf(
				"release server port %s host port = %q, want nonzero TCP port",
				containerPort,
				bindings[0].HostPort,
			)
		}
		addresses[index] = net.JoinHostPort(bindings[0].HostIP, bindings[0].HostPort)
	}
	return addresses[0], addresses[1]
}

func releaseOCIHTTPSClient(
	t *testing.T,
	stack *releaseOCIComposeStack,
	address string,
) (*http.Client, string) {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(releaseOCIReadFile(t, stack.values["OPEN_SPLUNK_SERVER_TLS_CA_FILE"]))) {
		t.Fatal("generated server CA contains no certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: stack.values["OPEN_SPLUNK_SERVER_TLS_SERVER_NAME"],
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, "https://" + address
}

func releaseOCIAssertHTTPSBoundary(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	httpAddress string,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request verified release health: %v", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 65))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("verified release health = status %d body %q read %v close %v", response.StatusCode, body, readErr, closeErr)
	}
	releaseOCIAssertPlaintextRejectedByTLS(t, ctx, httpAddress)
}

func releaseOCIAssertRuntimeReadinessFailureRecovery(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	client *http.Client,
	baseURL string,
	clickHouseID string,
) {
	t.Helper()
	status, body, err := releaseOCIProbeHealthEndpoint(
		ctx,
		client,
		baseURL+"/readyz",
	)
	if err != nil || status != http.StatusOK || body != "ok\n" {
		t.Fatalf(
			"initial runtime readiness = status %d body %q error %v",
			status,
			body,
			err,
		)
	}

	releaseOCIRunDocker(
		t,
		ctx,
		stack,
		"stop ClickHouse for readiness failure proof",
		"stop",
		"--time",
		"10",
		clickHouseID,
	)
	status, body, err = releaseOCIProbeHealthEndpoint(
		ctx,
		client,
		baseURL+"/readyz",
	)
	if err != nil || status != http.StatusServiceUnavailable ||
		body != "not ready\n" {
		t.Fatalf(
			"runtime readiness with ClickHouse stopped = status %d body %q error %v",
			status,
			body,
			err,
		)
	}
	status, body, err = releaseOCIProbeHealthEndpoint(
		ctx,
		client,
		baseURL+"/healthz",
	)
	if err != nil || status != http.StatusOK || body != "ok\n" {
		t.Fatalf(
			"process liveness with ClickHouse stopped = status %d body %q error %v",
			status,
			body,
			err,
		)
	}

	releaseOCIRunDocker(
		t,
		ctx,
		stack,
		"restart ClickHouse for readiness recovery proof",
		"start",
		clickHouseID,
	)
	recoveryContext, cancelRecovery := context.WithTimeout(ctx, 90*time.Second)
	defer cancelRecovery()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	// Match the migration gate so the official image's temporary init server
	// cannot masquerade as durable readiness after the container restart.
	const requiredConsecutiveReady = 6
	consecutiveReady := 0
	for {
		status, body, err = releaseOCIProbeHealthEndpoint(
			recoveryContext,
			client,
			baseURL+"/readyz",
		)
		if err == nil && status == http.StatusOK && body == "ok\n" {
			consecutiveReady++
			if consecutiveReady == requiredConsecutiveReady {
				return
			}
		} else {
			consecutiveReady = 0
		}
		select {
		case <-recoveryContext.Done():
			t.Fatalf(
				"runtime readiness did not recover after ClickHouse restart: status %d body %q error %v: %v",
				status,
				body,
				err,
				recoveryContext.Err(),
			)
		case <-ticker.C:
		}
	}
}

func releaseOCIProbeHealthEndpoint(
	ctx context.Context,
	client *http.Client,
	endpoint string,
) (int, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "", err
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 65))
	closeErr := response.Body.Close()
	return response.StatusCode, string(body), errors.Join(readErr, closeErr)
}

func releaseOCIAssertPlaintextRejectedByTLS(
	t *testing.T,
	ctx context.Context,
	httpAddress string,
) {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("plaintext release probe must not follow redirects")
		},
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://"+httpAddress+"/healthz",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("plaintext request to release TLS listener: %v", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 257))
	closeErr := response.Body.Close()
	const tlsListenerDiagnostic = "Client sent an HTTP request to an HTTPS server.\n"
	if readErr != nil || closeErr != nil ||
		response.StatusCode != http.StatusBadRequest ||
		string(body) != tlsListenerDiagnostic {
		t.Fatalf(
			"plaintext release TLS probe = status %d body %q read %v close %v",
			response.StatusCode,
			body,
			readErr,
			closeErr,
		)
	}
}

func releaseOCIAssertEmbeddedRelease(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("load release embedded UI: %v", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || len(body) > 2<<20 {
		t.Fatalf("embedded UI = status %d bytes %d read %v close %v", response.StatusCode, len(body), readErr, closeErr)
	}
	for _, expected := range [][]byte{
		[]byte("<title>Home | Open Splunk</title>"),
		[]byte(`data-open-splunk-version="` + stack.values["OPEN_SPLUNK_APPLICATION_VERSION"] + `"`),
		[]byte(`data-open-splunk-revision="` + stack.values["OPEN_SPLUNK_SOURCE_REVISION"] + `"`),
		[]byte("Backend mode selected"),
	} {
		if !bytes.Contains(body, expected) {
			t.Fatalf("embedded UI does not contain %q", expected)
		}
	}
	if bytes.Contains(body, []byte("Demo workspace ready")) {
		t.Fatal("release embedded UI was built in demo mode")
	}
	bootstrap := releaseOCIBootstrap(t, ctx, client, baseURL)
	build := bootstrap.GetBuild()
	if build.GetApplicationVersion() != stack.values["OPEN_SPLUNK_APPLICATION_VERSION"] ||
		build.GetSourceRevision() != stack.values["OPEN_SPLUNK_SOURCE_REVISION"] ||
		bootstrap.GetServerVersion() != stack.values["OPEN_SPLUNK_APPLICATION_VERSION"]+" ("+stack.values["OPEN_SPLUNK_SOURCE_REVISION"]+")" ||
		bootstrap.GetSplCompatibilityVersion() != spl.CompatibilityVersion ||
		build.GetUiBuildId() == "" || build.GetUiSha256() == "" ||
		build.GetAssetManifestFormatVersion() != 1 {
		t.Fatalf("release bootstrap identity = %+v", bootstrap)
	}
}

func releaseOCIBootstrap(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
) *opensplunkv1.GetSystemBootstrapResponse {
	t.Helper()
	result := &opensplunkv1.GetSystemBootstrapResponse{}
	postProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
		result,
	)
	return result
}

func releaseOCICreateIndex(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	name string,
) {
	t.Helper()
	request := &opensplunkv1.CreateIndexRequest{
		Definition: &opensplunkv1.IndexDefinition{
			Name:            name,
			DisplayName:     "Release OCI persistence proof",
			RetentionPeriod: durationpb.New(24 * time.Hour),
			IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
			SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		},
	}
	response := &opensplunkv1.CreateIndexResponse{}
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/indexes/create",
		administratorToken,
		request,
		response,
	)
	if response.GetIndex().GetDefinition().GetName() != name {
		t.Fatalf("created release OCI index = %+v, want %q", response.GetIndex(), name)
	}
}

func releaseOCIStateVolume(
	t *testing.T,
	container *releaseOCIContainerInspect,
) string {
	t.Helper()
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/lib/open-splunk/state" {
			if mount.Type != "volume" || mount.Name == "" || !mount.RW {
				t.Fatalf("server state mount = %+v, want writable named volume", mount)
			}
			return mount.Name
		}
	}
	t.Fatal("server has no state volume")
	return ""
}

func releaseOCIRunDocker(
	t *testing.T,
	ctx context.Context,
	stack *releaseOCIComposeStack,
	operation string,
	arguments ...string,
) string {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Dir = stack.repository
	command.Env = stack.environment()
	configureProcessGroup(command)
	output, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if err != nil || truncated {
		t.Fatalf(
			"%s: %v: %s",
			operation,
			err,
			stack.redact(formatBoundedCommandOutput(output, truncated, maximumHarnessOutputBytes)),
		)
	}
	return strings.TrimSpace(output)
}

func releaseOCIReadFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
}

func releaseOCIRotateClickHouseCredentials(
	t *testing.T,
	stack *releaseOCIComposeStack,
) {
	t.Helper()
	passwordKeys := []string{
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
	}
	used := make(map[string]struct{}, len(passwordKeys)*2)
	for _, key := range passwordKeys {
		oldPassword := stack.values[key]
		used[oldPassword] = struct{}{}
		stack.retainedSecrets = append(stack.retainedSecrets, oldPassword)
	}
	for _, key := range passwordKeys {
		var password string
		for {
			password = releaseOCIRandomHex(t, 32)
			if _, exists := used[password]; !exists {
				used[password] = struct{}{}
				break
			}
		}
		stack.values[key] = password
		fileKey := key + "_FILE"
		if path := stack.values[fileKey]; path != "" {
			releaseOCIReplaceCredentialFile(t, path, password)
		}
	}
}

func releaseOCIReplaceCredentialFile(t *testing.T, path, password string) {
	t.Helper()
	temporary := path + ".rotation-" + releaseOCIRandomHex(t, 6)
	if err := os.WriteFile(temporary, []byte(password), 0o600); err != nil {
		t.Fatalf("write rotated ClickHouse credential: %v", err)
	}
	if err := os.Chmod(temporary, 0o644); err != nil {
		t.Fatalf("set rotated ClickHouse credential mode: %v", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatalf("publish rotated ClickHouse credential: %v", err)
	}
}

func releaseOCIRandomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate release OCI suffix: %v", err)
	}
	return hex.EncodeToString(value)
}
