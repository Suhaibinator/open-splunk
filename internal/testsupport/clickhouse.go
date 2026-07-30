// Package testsupport contains reusable, opt-in integration-test fixtures.
package testsupport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const DefaultClickHouseImage = "clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49"

const (
	clickHouseMigrationUsername = "open_splunk_migrator"
	clickHouseRuntimeUsername   = "open_splunk_runtime"
	clickHouseDeletionUsername  = "open_splunk_deletion"
)

const servicePrincipalAccessConfig = `<clickhouse>
    <access_control_improvements>
        <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    </access_control_improvements>
</clickhouse>
`

// ClickHouseContainer is an ephemeral, loopback-only ClickHouse instance.
// Passwords are intentionally exposed only as data so callers can connect
// through the native driver; String must never be added because it could make
// accidental logging disclose credentials.
type ClickHouseContainer struct {
	Name              string
	Address           string
	Database          string
	Username          string
	Password          string
	MigrationUsername string
	MigrationPassword string
	RuntimeUsername   string
	RuntimePassword   string
	DeletionUsername  string
	DeletionPassword  string
	Image             string

	configDirectory   string
	bootstrapUsername string
	bootstrapPassword string
}

// StartClickHouse starts a disposable ClickHouse container and waits for four
// consecutive successful health probes. An empty image selects the pinned
// release used by the repository integration suite.
func StartClickHouse(ctx context.Context, image string) (*ClickHouseContainer, error) {
	if ctx == nil {
		return nil, errors.New("start ClickHouse test container: context is required")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: docker CLI is unavailable: %w", err)
	}
	if strings.TrimSpace(image) == "" {
		image = DefaultClickHouseImage
	}
	nameSuffix, err := randomHex(6)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: create name: %w", err)
	}
	password, err := randomHex(24)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: create password: %w", err)
	}
	container := &ClickHouseContainer{
		Name:     "open-splunk-clickhouse-" + nameSuffix,
		Database: "open_splunk",
		Username: "open_splunk",
		Password: password,
		Image:    image,
	}
	if output, err := docker(ctx,
		"run", "--detach", "--rm", "--name", container.Name,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB="+container.Database,
		"--env", "CLICKHOUSE_USER="+container.Username,
		"--env", "CLICKHOUSE_PASSWORD="+container.Password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		container.Image,
	); err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: %w: %s", err, boundedOutput(output))
	}
	started := true
	defer func() {
		if started {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = container.Close(cleanupCtx)
		}
	}()
	if err := container.waitReady(ctx); err != nil {
		return nil, err
	}
	address, err := container.nativeAddress(ctx)
	if err != nil {
		return nil, err
	}
	container.Address = address
	started = false
	return container, nil
}

// StartClickHouseWithServicePrincipals starts a disposable ClickHouse container
// whose schema does not yet exist, then provisions the least-privilege
// migration, runtime, and physical-deletion identities used in production.
// The bootstrap administrator is used only during fixture setup and is not
// returned to callers. An empty image selects the repository's pinned release.
func StartClickHouseWithServicePrincipals(ctx context.Context, image string) (*ClickHouseContainer, error) {
	if ctx == nil {
		return nil, errors.New("start ClickHouse service-principal test container: context is required")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: docker CLI is unavailable: %w", err)
	}
	if strings.TrimSpace(image) == "" {
		image = DefaultClickHouseImage
	}
	nameSuffix, err := randomHex(6)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create name: %w", err)
	}
	bootstrapPassword, err := randomHex(32)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create bootstrap password: %w", err)
	}
	migrationPassword, err := randomHex(32)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create migration password: %w", err)
	}
	runtimePassword, err := randomHex(32)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create runtime password: %w", err)
	}
	deletionPassword, err := randomHex(32)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create deletion password: %w", err)
	}
	provisioningSQL, err := servicePrincipalProvisioningSQL(
		migrationPassword,
		runtimePassword,
		deletionPassword,
	)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: build provisioning SQL: %w", err)
	}
	configDirectory, err := os.MkdirTemp("", "open-splunk-clickhouse-config-")
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse service-principal test container: create config directory: %w", err)
	}
	configPath := filepath.Join(configDirectory, "access.xml")
	// #nosec G306 -- the nonsecret config must be readable by the
	// unprivileged clickhouse user inside rootful Linux Docker.
	if err := os.WriteFile(configPath, []byte(servicePrincipalAccessConfig), 0o644); err != nil {
		_ = os.RemoveAll(configDirectory)
		return nil, fmt.Errorf("start ClickHouse service-principal test container: write access config: %w", err)
	}

	const bootstrapUsername = "open_splunk_bootstrap"
	container := &ClickHouseContainer{
		Name:              "open-splunk-clickhouse-principals-" + nameSuffix,
		Database:          "open_splunk",
		MigrationUsername: clickHouseMigrationUsername,
		MigrationPassword: migrationPassword,
		RuntimeUsername:   clickHouseRuntimeUsername,
		RuntimePassword:   runtimePassword,
		DeletionUsername:  clickHouseDeletionUsername,
		DeletionPassword:  deletionPassword,
		Image:             image,
		configDirectory:   configDirectory,
		bootstrapUsername: bootstrapUsername,
		bootstrapPassword: bootstrapPassword,
	}
	started := false
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		if !started {
			_ = os.RemoveAll(configDirectory)
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = container.Close(cleanupCtx)
	}()
	runArguments := servicePrincipalDockerArguments(
		container,
		configPath,
		bootstrapUsername,
		bootstrapPassword,
	)
	if output, runErr := docker(ctx, runArguments...); runErr != nil {
		return nil, fmt.Errorf(
			"start ClickHouse service-principal test container: %w: %s",
			runErr,
			boundedOutput(output),
		)
	}
	started = true
	if err := container.waitReadyWithCredentials(ctx, bootstrapUsername, bootstrapPassword); err != nil {
		return nil, err
	}
	if output, provisionErr := docker(ctx,
		"exec", container.Name, "clickhouse-client",
		"--user", bootstrapUsername, "--password", bootstrapPassword,
		"--multiquery", "--query", provisioningSQL,
	); provisionErr != nil {
		return nil, fmt.Errorf(
			"provision ClickHouse service principals: %w: %s",
			provisionErr,
			boundedOutput(output),
		)
	}
	address, err := container.nativeAddress(ctx)
	if err != nil {
		return nil, err
	}
	container.Address = address
	cleanup = false
	return container, nil
}

func servicePrincipalDockerArguments(
	container *ClickHouseContainer,
	configPath string,
	bootstrapUsername string,
	bootstrapPassword string,
) []string {
	return []string{
		"run", "--detach", "--rm", "--name", container.Name,
		"--publish", "127.0.0.1::9000",
		"--volume", configPath + ":/etc/clickhouse-server/config.d/open-splunk-access.xml:ro",
		"--env", "CLICKHOUSE_USER=" + bootstrapUsername,
		"--env", "CLICKHOUSE_PASSWORD=" + bootstrapPassword,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		container.Image,
	}
}

// ExecuteBootstrapSQLForTest runs access-control setup against an ephemeral
// service-principal fixture without exposing its bootstrap credential. It is
// intended only for adversarial integration proofs that temporarily add and
// revoke a grant.
func (container *ClickHouseContainer) ExecuteBootstrapSQLForTest(
	ctx context.Context,
	query string,
) error {
	if container == nil ||
		strings.TrimSpace(container.bootstrapUsername) == "" ||
		strings.TrimSpace(container.bootstrapPassword) == "" {
		return errors.New(
			"execute ClickHouse bootstrap SQL for test: service-principal fixture is required",
		)
	}
	if strings.TrimSpace(query) == "" {
		return errors.New(
			"execute ClickHouse bootstrap SQL for test: query is required",
		)
	}
	output, err := docker(
		ctx,
		"exec",
		container.Name,
		"clickhouse-client",
		"--user",
		container.bootstrapUsername,
		"--password",
		container.bootstrapPassword,
		"--multiquery",
		"--query",
		query,
	)
	if err != nil {
		return fmt.Errorf(
			"execute ClickHouse bootstrap SQL for test: %w: %s",
			err,
			boundedOutput(output),
		)
	}
	return nil
}

// Close forcibly removes the disposable container. Docker --rm makes this
// idempotent after a process or daemon has already removed it. It also removes
// any temporary server configuration owned by an opt-in fixture.
func (container *ClickHouseContainer) Close(ctx context.Context) error {
	if container == nil {
		return nil
	}
	var closeErrors []error
	if strings.TrimSpace(container.Name) != "" {
		output, err := docker(ctx, "rm", "--force", container.Name)
		if err != nil && !strings.Contains(string(output), "No such container") {
			closeErrors = append(closeErrors, fmt.Errorf(
				"remove ClickHouse test container: %w: %s",
				err,
				boundedOutput(output),
			))
		}
	}
	if strings.TrimSpace(container.configDirectory) != "" {
		if err := os.RemoveAll(container.configDirectory); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("remove ClickHouse test config: %w", err))
		} else {
			container.configDirectory = ""
		}
	}
	container.bootstrapUsername = ""
	container.bootstrapPassword = ""
	return errors.Join(closeErrors...)
}

func (container *ClickHouseContainer) waitReady(ctx context.Context) error {
	return container.waitReadyWithCredentials(ctx, container.Username, container.Password)
}

func (container *ClickHouseContainer) waitReadyWithCredentials(
	ctx context.Context,
	username string,
	password string,
) error {
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	var last string
	for {
		output, err := docker(ctx, "exec", container.Name, "clickhouse-client",
			"--user", username, "--password", password,
			"--query", "SELECT 1",
		)
		last = fmt.Sprintf("%v: %s", err, boundedOutput(output))
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			stable++
			if stable == 4 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for ClickHouse test container: %w", ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("wait for ClickHouse test container: timed out: %s", last)
		case <-ticker.C:
		}
	}
}

func servicePrincipalProvisioningSQL(
	migrationPassword string,
	runtimePassword string,
	deletionPassword string,
) (string, error) {
	for name, password := range map[string]string{
		"migration password": migrationPassword,
		"runtime password":   runtimePassword,
		"deletion password":  deletionPassword,
	} {
		if !isLowerHexCredential(password) {
			return "", fmt.Errorf("%s must contain exactly 64 lowercase hexadecimal characters", name)
		}
	}
	return fmt.Sprintf(`CREATE USER IF NOT EXISTS open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '%s';
ALTER USER open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '%s';
GRANT CREATE DATABASE ON open_splunk.* TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator;
GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX ON open_splunk.events TO open_splunk_migrator;
GRANT SELECT ON system.tables TO open_splunk_migrator;
GRANT SELECT, INSERT ON open_splunk.schema_migrations TO open_splunk_migrator;

CREATE USER IF NOT EXISTS open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '%s';
ALTER USER open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '%s';
GRANT SELECT, INSERT ON open_splunk.events TO open_splunk_runtime;
GRANT SELECT(database, table, active, rows, bytes_on_disk) ON system.parts TO open_splunk_runtime;

CREATE USER IF NOT EXISTS open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '%s';
ALTER USER open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '%s';
GRANT ALTER DELETE, SELECT(tenant_id, index_name) ON open_splunk.events TO open_splunk_deletion;
GRANT SELECT ON system.tables TO open_splunk_deletion;
GRANT SELECT ON system.mutations TO open_splunk_deletion;
`,
		migrationPassword,
		migrationPassword,
		runtimePassword,
		runtimePassword,
		deletionPassword,
		deletionPassword,
	), nil
}

func isLowerHexCredential(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func (container *ClickHouseContainer) nativeAddress(ctx context.Context) (string, error) {
	output, err := docker(ctx, "port", container.Name, "9000/tcp")
	if err != nil {
		return "", fmt.Errorf("resolve ClickHouse test native port: %w: %s", err, boundedOutput(output))
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line, nil
		}
	}
	return "", fmt.Errorf("resolve ClickHouse test native port: no loopback mapping in %q", boundedOutput(output))
}

func docker(ctx context.Context, arguments ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.WaitDelay = 5 * time.Second
	return command.CombinedOutput()
}

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func boundedOutput(output []byte) string {
	const maximum = 4 << 10
	if len(output) > maximum {
		output = output[len(output)-maximum:]
	}
	return strings.TrimSpace(string(output))
}
