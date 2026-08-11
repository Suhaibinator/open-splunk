package clickhouse_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	shippedmigrations "github.com/Suhaibinator/open-splunk/migrations"
)

const (
	composeBootstrapUsername              = "open_splunk_bootstrap"
	composeMigrationUsername              = "open_splunk_migrator"
	composeRuntimeUsername                = "open_splunk_runtime"
	composeDeletionUsername               = "open_splunk_deletion"
	composeBackupUsername                 = "open_splunk_backup"
	composeRestoreUsername                = "open_splunk_restore"
	composeExcessRoleName                 = "open_splunk_integration_excess_role"
	maximumDeploymentComposeCommandOutput = 1 << 20
)

var errDeploymentComposeOutputTruncated = errors.New("command output exceeded 1 MiB limit")

func TestDeploymentComposeOutputBufferIsBounded(t *testing.T) {
	output := &deploymentComposeOutputBuffer{maximum: 4}
	written, err := output.Write([]byte("abcdef"))
	if err != nil || written != 6 {
		t.Fatalf("Write() = (%d, %v), want (6, nil)", written, err)
	}
	captured, truncated := output.snapshot()
	if string(captured) != "abcd" || !truncated {
		t.Fatalf("snapshot() = (%q, %t), want (abcd, true)", captured, truncated)
	}
}

func TestDeploymentComposeRecoveryBootstrapOverrideContract(t *testing.T) {
	t.Parallel()

	override := readFile(
		t,
		filepath.Join(deploymentDirectory(t), "docker-compose.integration.yaml"),
	)
	for _, fragment := range []string{
		"clickhouse-recovery-volume-bootstrap:",
		"image: " + testsupport.DefaultClickHouseImage,
		"pull_policy: missing",
		"user: \"0:65532\"",
		"entrypoint:",
		"- /bin/sh",
		"chown 101:65532 /var/lib/open-splunk/clickhouse-backups",
		"chmod 2750 /var/lib/open-splunk/clickhouse-backups",
	} {
		if !strings.Contains(override, fragment) {
			t.Errorf("Compose integration recovery bootstrap is missing %q", fragment)
		}
	}
	for _, prohibited := range []string{"chmod -R", "chown -R", ":latest", ":lts"} {
		if strings.Contains(override, prohibited) {
			t.Errorf("Compose integration recovery bootstrap contains %q", prohibited)
		}
	}
}

// TestDeploymentComposePersistentCredentialRotation is opt-in because it
// exercises the checked-in Compose deployment, including its persistent named
// volume. It proves that the always-run initialization path remains
// idempotent and reapplies every rotated credential on an existing database.
func TestDeploymentComposePersistentCredentialRotation(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip(
			"set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker Compose integration test",
		)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker integration was requested but the CLI is unavailable: %v", err)
	}
	composeProbeContext, composeProbeCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer composeProbeCancel()
	composeProbe := exec.CommandContext(
		composeProbeContext,
		"docker",
		"compose",
		"version",
	)
	composeProbe.WaitDelay = 5 * time.Second
	if output, err := runDeploymentComposeCommand(composeProbe); err != nil {
		t.Fatalf(
			"Docker integration was requested but Compose is unavailable: %v: %s",
			err,
			boundedComposeOutput(output),
		)
	}

	deployDirectory := deploymentDirectory(t)
	credentials, generatedEnvironment := generateDeploymentComposeEnvironment(
		t,
		deployDirectory,
	)
	stack := &deploymentComposeStack{
		project:              "open-splunk-compose-test-" + randomHex(t, 6),
		projectDirectory:     deployDirectory,
		composeFile:          filepath.Join(deployDirectory, "docker-compose.yaml"),
		composeOverrides:     []string{filepath.Join(deployDirectory, "docker-compose.integration.yaml")},
		credentials:          credentials,
		generatedEnvironment: generatedEnvironment,
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			45*time.Second,
		)
		defer cleanupCancel()
		output, cleanupErr := stack.run(
			cleanupContext,
			"down",
			"--volumes",
			"--remove-orphans",
			"--timeout",
			"10",
		)
		if cleanupErr != nil {
			t.Errorf(
				"remove Docker Compose test project %q: %v: %s",
				stack.project,
				cleanupErr,
				boundedComposeOutput(output),
			)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	images := stack.mustRun(t, ctx, "resolve deployment image", "config", "--images", "clickhouse")
	requireDigestPinnedComposeImage(t, images)

	stack.mustRun(
		t,
		ctx,
		"start fresh deployment",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"180",
		"clickhouse",
	)
	firstContainerID := strings.TrimSpace(
		stack.mustRun(t, ctx, "resolve first container", "ps", "--quiet", "clickhouse"),
	)
	if firstContainerID == "" {
		t.Fatal("fresh Docker Compose deployment returned an empty container ID")
	}
	firstDataVolume := inspectComposeDataVolume(t, ctx, firstContainerID)
	firstAddress := stack.publishedAddress(t, ctx, firstContainerID, "9440")

	verifyComposeRecoveryVolumeOwnership(t, ctx, stack)
	verifyComposeBootstrapBoundary(t, ctx, stack, firstAddress)
	verifyComposeTLSBoundary(t, ctx, stack, firstAddress)
	markerEventID := "compose-persistence-" + randomHex(t, 8)
	validateComposePrincipalsAndSchema(
		t,
		ctx,
		stack,
		firstAddress,
		stack.credentials,
		markerEventID,
		true,
	)
	seedComposeExcessPrincipalAuthority(t, ctx, stack, firstAddress)

	oldCredentials := stack.credentials
	stack.credentials = newDeploymentComposeCredentials(
		t,
		oldCredentials.passwordSet(),
	)
	stack.mustRun(
		t,
		ctx,
		"force-recreate deployment with rotated credentials",
		"up",
		"--detach",
		"--wait",
		"--wait-timeout",
		"180",
		"--force-recreate",
		"clickhouse",
	)
	secondContainerID := strings.TrimSpace(
		stack.mustRun(t, ctx, "resolve recreated container", "ps", "--quiet", "clickhouse"),
	)
	if secondContainerID == "" {
		t.Fatal("recreated Docker Compose deployment returned an empty container ID")
	}
	if secondContainerID == firstContainerID {
		t.Fatalf(
			"force-recreate retained container %q; want a newly created container",
			secondContainerID,
		)
	}
	secondDataVolume := inspectComposeDataVolume(t, ctx, secondContainerID)
	if secondDataVolume != firstDataVolume {
		t.Fatalf(
			"ClickHouse data volume after force-recreate = %q, want persistent volume %q",
			secondDataVolume,
			firstDataVolume,
		)
	}
	secondAddress := stack.publishedAddress(t, ctx, secondContainerID, "9440")

	verifyComposeRecoveryVolumeOwnership(t, ctx, stack)
	expectComposeBootstrapPasswordRejected(
		t,
		ctx,
		stack,
		oldCredentials.bootstrapPassword,
	)
	expectComposePrincipalCredentialsRejected(
		t,
		ctx,
		stack,
		secondAddress,
		oldCredentials,
	)
	verifyComposeBootstrapBoundary(t, ctx, stack, secondAddress)
	validateComposePrincipalsAndSchema(
		t,
		ctx,
		stack,
		secondAddress,
		stack.credentials,
		markerEventID,
		false,
	)
	verifyComposeExcessPrincipalAuthorityRemoved(t, ctx, stack, secondAddress)
}

type deploymentComposeCredentials struct {
	bootstrapPassword string
	migrationPassword string
	runtimePassword   string
	deletionPassword  string
	backupPassword    string
	restorePassword   string
}

type deploymentComposeClientTLSIdentity struct {
	rootCAs    *x509.CertPool
	serverName string
}

type deploymentComposeGeneratedEnvironment struct {
	values        map[string]string
	clickHouseTLS deploymentComposeClientTLSIdentity
}

func generateDeploymentComposeEnvironment(
	t *testing.T,
	deployDirectory string,
) (deploymentComposeCredentials, *deploymentComposeGeneratedEnvironment) {
	t.Helper()
	envFile := filepath.Join(t.TempDir(), "open-splunk.env")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	values := mustGenerateDeploymentEnvironment(
		t,
		ctx,
		deployDirectory,
		envFile,
	)
	caCertificateFile := values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"]
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(readFile(t, caCertificateFile))) {
		t.Fatal("generated deployment CA bundle contains no certificates")
	}
	credentials := deploymentComposeCredentials{
		bootstrapPassword: values["OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD"],
		migrationPassword: values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD"],
		runtimePassword:   values["OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD"],
		deletionPassword:  values["OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD"],
		backupPassword:    values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD"],
		restorePassword:   values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"],
	}
	for name, password := range map[string]string{
		"bootstrap": credentials.bootstrapPassword,
		"migration": credentials.migrationPassword,
		"runtime":   credentials.runtimePassword,
		"deletion":  credentials.deletionPassword,
		"backup":    credentials.backupPassword,
		"restore":   credentials.restorePassword,
	} {
		if len(password) != 64 || strings.Trim(password, "0123456789abcdef") != "" {
			t.Fatalf("generated deployment %s password is invalid", name)
		}
	}
	return credentials, &deploymentComposeGeneratedEnvironment{
		values: values,
		clickHouseTLS: deploymentComposeClientTLSIdentity{
			rootCAs:    roots,
			serverName: values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		},
	}
}

func newDeploymentComposeCredentials(
	t *testing.T,
	excluded map[string]struct{},
) deploymentComposeCredentials {
	t.Helper()
	used := make(map[string]struct{}, len(excluded)+6)
	for password := range excluded {
		used[password] = struct{}{}
	}
	nextPassword := func() string {
		for {
			password := randomHex(t, 32)
			if _, exists := used[password]; exists {
				continue
			}
			used[password] = struct{}{}
			return password
		}
	}
	return deploymentComposeCredentials{
		bootstrapPassword: nextPassword(),
		migrationPassword: nextPassword(),
		runtimePassword:   nextPassword(),
		deletionPassword:  nextPassword(),
		backupPassword:    nextPassword(),
		restorePassword:   nextPassword(),
	}
}

func (credentials deploymentComposeCredentials) passwordSet() map[string]struct{} {
	return map[string]struct{}{
		credentials.bootstrapPassword: {},
		credentials.migrationPassword: {},
		credentials.runtimePassword:   {},
		credentials.deletionPassword:  {},
		credentials.backupPassword:    {},
		credentials.restorePassword:   {},
	}
}

type deploymentComposeStack struct {
	project              string
	projectDirectory     string
	composeFile          string
	composeOverrides     []string
	credentials          deploymentComposeCredentials
	generatedEnvironment *deploymentComposeGeneratedEnvironment
}

func (stack *deploymentComposeStack) run(
	ctx context.Context,
	arguments ...string,
) ([]byte, error) {
	return stack.runWithEnvironment(ctx, nil, arguments...)
}

func (stack *deploymentComposeStack) runWithEnvironment(
	ctx context.Context,
	additionalEnvironment map[string]string,
	arguments ...string,
) ([]byte, error) {
	composeArguments := []string{
		"compose",
		"--project-name",
		stack.project,
		"--project-directory",
		stack.projectDirectory,
		"--file",
		stack.composeFile,
	}
	for _, override := range stack.composeOverrides {
		composeArguments = append(composeArguments, "--file", override)
	}
	composeArguments = append(composeArguments, arguments...)
	command := exec.CommandContext(ctx, "docker", composeArguments...)
	command.Env = stack.environment(additionalEnvironment)
	command.WaitDelay = 5 * time.Second
	return runDeploymentComposeCommand(command)
}

type deploymentComposeOutputBuffer struct {
	mutex     sync.Mutex
	output    []byte
	maximum   int
	truncated bool
}

func (output *deploymentComposeOutputBuffer) Write(value []byte) (int, error) {
	output.mutex.Lock()
	defer output.mutex.Unlock()

	written := len(value)
	remaining := output.maximum - len(output.output)
	if remaining > len(value) {
		remaining = len(value)
	}
	if remaining > 0 {
		output.output = append(output.output, value[:remaining]...)
	}
	if remaining < len(value) {
		output.truncated = true
	}
	return written, nil
}

func (output *deploymentComposeOutputBuffer) snapshot() ([]byte, bool) {
	output.mutex.Lock()
	defer output.mutex.Unlock()
	return append([]byte(nil), output.output...), output.truncated
}

func runDeploymentComposeCommand(command *exec.Cmd) ([]byte, error) {
	output := &deploymentComposeOutputBuffer{
		maximum: maximumDeploymentComposeCommandOutput,
	}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	captured, truncated := output.snapshot()
	if truncated {
		err = errors.Join(err, errDeploymentComposeOutputTruncated)
	}
	return captured, err
}

func (stack *deploymentComposeStack) environment(
	additionalEnvironment map[string]string,
) []string {
	values := maps.Clone(stack.generatedEnvironment.values)
	values["COMPOSE_ANSI"] = "never"
	values["OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD"] = stack.credentials.bootstrapPassword
	values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD"] = stack.credentials.migrationPassword
	values["OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD"] = stack.credentials.runtimePassword
	values["OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD"] = stack.credentials.deletionPassword
	values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD"] = stack.credentials.backupPassword
	values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"] = stack.credentials.restorePassword
	values["OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT"] = "0"
	values["OPEN_SPLUNK_SERVER_HTTP_PORT"] = "0"
	values["OPEN_SPLUNK_SERVER_GRPC_PORT"] = "0"
	for key, value := range additionalEnvironment {
		values[key] = value
	}

	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := values[key]; !replaced {
			environment = append(environment, entry)
		}
	}
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	return environment
}

func (stack *deploymentComposeStack) mustRun(
	t *testing.T,
	ctx context.Context,
	operation string,
	arguments ...string,
) string {
	t.Helper()
	output, err := stack.run(ctx, arguments...)
	if err != nil {
		logOutput, _ := stack.run(ctx, "logs", "--no-color", "--tail", "200", "clickhouse")
		t.Fatalf(
			"%s: %v\noutput:\n%s\nClickHouse logs:\n%s",
			operation,
			err,
			boundedComposeOutput(output),
			boundedComposeOutput(logOutput),
		)
	}
	return strings.TrimSpace(string(output))
}

func (stack *deploymentComposeStack) publishedAddress(
	t *testing.T,
	ctx context.Context,
	containerID string,
	containerPort string,
) string {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", "port", containerID, containerPort+"/tcp")
	command.WaitDelay = 5 * time.Second
	rawOutput, err := runDeploymentComposeCommand(command)
	if err != nil {
		t.Fatalf(
			"resolve published ClickHouse port %s: %v: %s",
			containerPort,
			err,
			boundedComposeOutput(rawOutput),
		)
	}
	output := strings.TrimSpace(string(rawOutput))
	lines := strings.Fields(output)
	if len(lines) != 1 {
		t.Fatalf(
			"published ClickHouse port %s = %q, want one address",
			containerPort,
			output,
		)
	}
	host, port, err := net.SplitHostPort(lines[0])
	if err != nil {
		t.Fatalf("parse published ClickHouse port %s: %v", containerPort, err)
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		t.Fatalf(
			"published ClickHouse port %s = %q, want a Docker-assigned nonzero port",
			containerPort,
			port,
		)
	}
	if host != "127.0.0.1" {
		t.Fatalf(
			"published ClickHouse port %s host = %q, want loopback-only 127.0.0.1",
			containerPort,
			host,
		)
	}
	return net.JoinHostPort(host, port)
}

func requireDigestPinnedComposeImage(t *testing.T, output string) {
	t.Helper()
	images := strings.Fields(output)
	if len(images) == 0 {
		t.Fatal("Docker Compose resolved no images for ClickHouse and its dependencies")
	}
	for _, image := range images {
		digestIndex := strings.LastIndex(image, "@sha256:")
		if image != testsupport.DefaultClickHouseImage || digestIndex <= 0 ||
			len(image[digestIndex+len("@sha256:"):]) != 64 ||
			strings.Trim(image[digestIndex+len("@sha256:"):], "0123456789abcdef") != "" {
			t.Fatalf(
				"ClickHouse integration image must use the exact digest pin %q, got %q",
				testsupport.DefaultClickHouseImage,
				image,
			)
		}
	}
}

func inspectComposeDataVolume(
	t *testing.T,
	ctx context.Context,
	containerID string,
) string {
	t.Helper()
	command := exec.CommandContext(
		ctx,
		"docker",
		"inspect",
		"--format",
		`{{range .Mounts}}{{if eq .Destination "/var/lib/clickhouse"}}{{.Type}}:{{.Name}}{{end}}{{end}}`,
		containerID,
	)
	command.WaitDelay = 5 * time.Second
	output, err := runDeploymentComposeCommand(command)
	if err != nil {
		t.Fatalf(
			"inspect ClickHouse data mount: %v: %s",
			err,
			boundedComposeOutput(output),
		)
	}
	mount := strings.TrimSpace(string(output))
	const volumePrefix = "volume:"
	if !strings.HasPrefix(mount, volumePrefix) ||
		strings.TrimPrefix(mount, volumePrefix) == "" {
		t.Fatalf(
			"ClickHouse /var/lib/clickhouse mount = %q, want a named volume",
			mount,
		)
	}
	return strings.TrimPrefix(mount, volumePrefix)
}

func verifyComposeRecoveryVolumeOwnership(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
) {
	t.Helper()
	const recoveryPath = "/var/lib/open-splunk-clickhouse-backups"
	ownership := stack.mustRun(
		t,
		ctx,
		"inspect ClickHouse recovery volume ownership",
		"exec",
		"--no-TTY",
		"clickhouse",
		"stat",
		"--format=%u:%g:%a",
		recoveryPath,
	)
	if ownership != "101:65532:2750" {
		t.Fatalf(
			"ClickHouse recovery volume ownership and mode = %q, want 101:65532:2750",
			ownership,
		)
	}
}

func verifyComposeBootstrapBoundary(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
) {
	t.Helper()
	defaultConnection := openDeploymentComposeConnection(
		t,
		address,
		"default",
		"default",
		"",
		stack.clientTLSConfig(),
	)
	if err := defaultConnection.Ping(ctx); err == nil {
		_ = defaultConnection.Close()
		t.Fatal("base ClickHouse default principal unexpectedly connected")
	}
	if err := defaultConnection.Close(); err != nil {
		t.Fatalf("close denied ClickHouse default-principal connection: %v", err)
	}

	connection := openDeploymentComposeConnection(
		t,
		address,
		"default",
		composeBootstrapUsername,
		stack.credentials.bootstrapPassword,
		stack.clientTLSConfig(),
	)
	if err := connection.Ping(ctx); err == nil {
		_ = connection.Close()
		t.Fatal("bootstrap principal unexpectedly connected over the published native port")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close denied published bootstrap connection: %v", err)
	}

	const query = `clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --query "SELECT 1"`
	output, err := stack.run(ctx, "exec", "--no-TTY", "clickhouse", "sh", "-ec", query)
	if err != nil {
		t.Fatalf(
			"query as bootstrap principal through docker compose exec: %v: %s",
			err,
			boundedComposeOutput(output),
		)
	}
	if strings.TrimSpace(string(output)) != "1" {
		t.Fatalf(
			"bootstrap docker compose exec query = %q, want 1",
			strings.TrimSpace(string(output)),
		)
	}
}

func seedComposeExcessPrincipalAuthority(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
) {
	t.Helper()
	stack.mustRun(
		t,
		ctx,
		"seed excess direct and role-derived runtime authority",
		"exec",
		"--no-TTY",
		"clickhouse",
		"clickhouse-client",
		"--host",
		"127.0.0.1",
		"--user",
		composeBootstrapUsername,
		"--multiquery",
		"--query",
		"CREATE ROLE "+composeExcessRoleName+"; "+
			"GRANT DROP TABLE ON open_splunk.events TO "+composeRuntimeUsername+"; "+
			"GRANT SELECT ON system.users TO "+composeExcessRoleName+"; "+
			"GRANT "+composeExcessRoleName+" TO "+composeRuntimeUsername,
	)

	connection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeRuntimeUsername,
		stack.credentials.runtimePassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "excess-authority runtime", connection)
	assertComposeGrantCheck(t, ctx, connection, "DROP TABLE ON open_splunk.events", 1)
	assertComposeGrantCheck(t, ctx, connection, "SELECT ON system.users", 1)
}

func verifyComposeExcessPrincipalAuthorityRemoved(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
) {
	t.Helper()
	connection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeRuntimeUsername,
		stack.credentials.runtimePassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "reset-authority runtime", connection)
	assertComposeGrantCheck(t, ctx, connection, "DROP TABLE ON open_splunk.events", 0)
	assertComposeGrantCheck(t, ctx, connection, "SELECT ON system.users", 0)

	roleGrants := stack.mustRun(
		t,
		ctx,
		"prove stale runtime role assignment was removed",
		"exec",
		"--no-TTY",
		"clickhouse",
		"clickhouse-client",
		"--host",
		"127.0.0.1",
		"--user",
		composeBootstrapUsername,
		"--query",
		"SELECT count() FROM system.role_grants WHERE user_name = '"+
			composeRuntimeUsername+"' AND granted_role_name = '"+
			composeExcessRoleName+"' FORMAT TSVRaw",
	)
	if roleGrants != "0" {
		t.Fatalf("stale runtime role assignment count = %q, want 0", roleGrants)
	}
}

func assertComposeGrantCheck(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	grant string,
	want uint8,
) {
	t.Helper()
	var got uint8
	if err := connection.QueryRow(ctx, "CHECK GRANT "+grant).Scan(&got); err != nil {
		t.Fatalf("check deployment principal grant %q: %v", grant, err)
	}
	if got != want {
		t.Fatalf("deployment principal grant %q = %d, want %d", grant, got, want)
	}
}

func verifyComposeTLSBoundary(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
) {
	t.Helper()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	verifiedDialer := &tls.Dialer{
		NetDialer: dialer,
		Config:    stack.clientTLSConfig(),
	}
	rawTLSConnection, err := verifiedDialer.DialContext(
		ctx,
		"tcp",
		address,
	)
	if err != nil {
		t.Fatalf("perform verified ClickHouse TLS handshake: %v", err)
	}
	tlsConnection, ok := rawTLSConnection.(*tls.Conn)
	if !ok {
		_ = rawTLSConnection.Close()
		t.Fatalf("verified ClickHouse TLS connection type = %T", rawTLSConnection)
	}
	state := tlsConnection.ConnectionState()
	if state.Version < tls.VersionTLS12 || len(state.PeerCertificates) == 0 ||
		state.ServerName != stack.generatedEnvironment.clickHouseTLS.serverName {
		_ = tlsConnection.Close()
		t.Fatalf("verified ClickHouse TLS state = %#v", state)
	}
	if err := tlsConnection.Close(); err != nil {
		t.Fatalf("close verified ClickHouse TLS connection: %v", err)
	}
	legacyTLS := stack.clientTLSConfig()
	legacyTLS.MinVersion = tls.VersionTLS10 // #nosec G402 -- rejection probe.
	legacyTLS.MaxVersion = tls.VersionTLS11
	legacyDialer := &tls.Dialer{
		NetDialer: dialer,
		Config:    legacyTLS,
	}
	if legacyConnection, legacyErr := legacyDialer.DialContext(
		ctx,
		"tcp",
		address,
	); legacyErr == nil {
		_ = legacyConnection.Close()
		t.Fatal("ClickHouse TLS listener unexpectedly accepted TLS 1.1")
	}

	wrongName := stack.clientTLSConfig()
	wrongName.ServerName = "wrong-clickhouse.internal"
	expectDeploymentComposeConnectionRejected(
		t,
		ctx,
		address,
		stack.credentials.runtimePassword,
		wrongName,
		"wrong ClickHouse TLS server name",
	)
	wrongIdentity, err := testsupport.WriteServerTLSIdentity(
		t.TempDir(),
		stack.generatedEnvironment.clickHouseTLS.serverName,
	)
	if err != nil {
		t.Fatalf("create wrong ClickHouse TLS trust root: %v", err)
	}
	wrongTrust := stack.clientTLSConfig()
	wrongTrust.RootCAs = wrongIdentity.RootCAs
	expectDeploymentComposeConnectionRejected(
		t,
		ctx,
		address,
		stack.credentials.runtimePassword,
		wrongTrust,
		"wrong ClickHouse TLS trust root",
	)
	expectDeploymentComposeConnectionRejected(
		t,
		ctx,
		address,
		stack.credentials.runtimePassword,
		nil,
		"plaintext on ClickHouse TLS listener",
	)
}

func expectDeploymentComposeConnectionRejected(
	t *testing.T,
	ctx context.Context,
	address string,
	password string,
	tlsConfig *tls.Config,
	name string,
) {
	t.Helper()
	connection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeRuntimeUsername,
		password,
		tlsConfig,
	)
	if err := connection.Ping(ctx); err == nil {
		_ = connection.Close()
		t.Fatalf("%s unexpectedly connected", name)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close rejected %s connection: %v", name, err)
	}
}

func (stack *deploymentComposeStack) clientTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    stack.generatedEnvironment.clickHouseTLS.rootCAs.Clone(),
		ServerName: stack.generatedEnvironment.clickHouseTLS.serverName,
	}
}

func expectComposeBootstrapPasswordRejected(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	oldPassword string,
) {
	t.Helper()
	const passwordEnvironment = "CLICKHOUSE_PASSWORD"
	const query = `clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --query "SELECT 1"`
	output, err := stack.runWithEnvironment(
		ctx,
		map[string]string{passwordEnvironment: oldPassword},
		"exec",
		"--no-TTY",
		"--env",
		passwordEnvironment,
		"clickhouse",
		"sh",
		"-ec",
		query,
	)
	if err == nil {
		t.Fatalf(
			"old bootstrap credential unexpectedly succeeded through docker compose exec: %s",
			boundedComposeOutput(output),
		)
	}
	if errors.Is(err, errDeploymentComposeOutputTruncated) {
		t.Fatalf(
			"old bootstrap credential probe produced excessive output: %s",
			boundedComposeOutput(output),
		)
	}
	if !isClickHouseAuthenticationFailure(string(output)) {
		t.Fatalf(
			"old bootstrap credential error = %v: %s, want authentication failure",
			err,
			boundedComposeOutput(output),
		)
	}
}

func validateComposePrincipalsAndSchema(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
	credentials deploymentComposeCredentials,
	markerEventID string,
	insertMarker bool,
) {
	t.Helper()
	migrationConnection := openDeploymentComposeConnection(
		t,
		address,
		"default",
		composeMigrationUsername,
		credentials.migrationPassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "migration", migrationConnection)
	if err := migrationConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deployment migration principal: %v", err)
	}
	if err := server.ValidateClickHouseMigrationPrivileges(
		ctx,
		migrationConnection,
	); err != nil {
		t.Fatalf("validate deployment migration principal: %v", err)
	}
	if err := server.ApplyClickHouseMigrations(
		ctx,
		migrationConnection,
		shippedmigrations.ClickHouse(),
	); err != nil {
		t.Fatalf("validate deployment migration ledger and schema: %v", err)
	}

	runtimeConnection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeRuntimeUsername,
		credentials.runtimePassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "runtime", runtimeConnection)
	if err := runtimeConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deployment runtime principal: %v", err)
	}
	if err := server.ValidateClickHouseRuntimePrivileges(
		ctx,
		runtimeConnection,
	); err != nil {
		t.Fatalf("validate deployment runtime principal: %v", err)
	}
	var activeRows, activeBytes uint64
	if err := runtimeConnection.QueryRow(
		ctx,
		`SELECT
		     coalesce(sum(rows), 0),
		     coalesce(sum(bytes_on_disk), 0)
		 FROM system.parts
		 WHERE database = ?
		   AND table = ?
		   AND active = 1`,
		"open_splunk",
		"events",
	).Scan(&activeRows, &activeBytes); err != nil {
		t.Fatalf(
			"query deployment index statistics through runtime principal: %v",
			err,
		)
	}
	var broadPartsGrant, unrelatedPartsColumnGrant uint8
	if err := runtimeConnection.QueryRow(
		ctx,
		"CHECK GRANT SELECT ON system.parts",
	).Scan(&broadPartsGrant); err != nil {
		t.Fatalf("check broad runtime system.parts grant: %v", err)
	}
	if err := runtimeConnection.QueryRow(
		ctx,
		"CHECK GRANT SELECT(name) ON system.parts",
	).Scan(&unrelatedPartsColumnGrant); err != nil {
		t.Fatalf("check unrelated runtime system.parts column grant: %v", err)
	}
	if broadPartsGrant != 0 || unrelatedPartsColumnGrant != 0 {
		t.Fatalf(
			"runtime system.parts excess grants = broad:%d name:%d, want 0/0",
			broadPartsGrant,
			unrelatedPartsColumnGrant,
		)
	}

	deletionConnection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeDeletionUsername,
		credentials.deletionPassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "deletion", deletionConnection)
	if err := deletionConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deployment deletion principal: %v", err)
	}
	if err := server.ValidateClickHouseDeletionWorkerPrivileges(
		ctx,
		deletionConnection,
	); err != nil {
		t.Fatalf("validate deployment deletion principal: %v", err)
	}

	backupConnection := openDeploymentComposeConnection(
		t,
		address,
		"open_splunk",
		composeBackupUsername,
		credentials.backupPassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "backup", backupConnection)
	if err := backupConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deployment backup principal: %v", err)
	}
	if err := server.ValidateClickHouseBackupPrivileges(ctx, backupConnection); err != nil {
		t.Fatalf("validate deployment backup principal: %v", err)
	}

	restoreConnection := openDeploymentComposeConnection(
		t,
		address,
		"default",
		composeRestoreUsername,
		credentials.restorePassword,
		stack.clientTLSConfig(),
	)
	defer closeDeploymentComposeConnection(t, "restore", restoreConnection)
	if err := restoreConnection.Ping(ctx); err != nil {
		t.Fatalf("ping deployment restore principal: %v", err)
	}
	if err := server.ValidateClickHouseRestorePrivileges(ctx, restoreConnection); err != nil {
		t.Fatalf("validate deployment restore principal: %v", err)
	}

	const (
		markerTenant = "compose-persistence-tenant"
		markerIndex  = "compose-persistence-index"
	)
	if insertMarker {
		source := ingest.NativeCollectorSource("compose-persistence-fixture-collector")
		if err := runtimeConnection.Exec(
			ctx,
			`INSERT INTO open_splunk.events
				(
					event_id, tenant_id, index_name, event_time, index_time,
					collector_id, ingest_source_kind, ingest_source_id,
					expires_at, visibility_seq
				)
			 VALUES (?, ?, ?, now64(9), now64(3), ?, ?, ?,
			         now64(3) + INTERVAL 1 DAY, 1)`,
			markerEventID,
			markerTenant,
			markerIndex,
			source.CollectorID,
			uint8(source.Kind),
			source.ID,
		); err != nil {
			t.Fatalf("insert persistent Compose marker event: %v", err)
		}
	}
	var markerCount uint64
	if err := runtimeConnection.QueryRow(
		ctx,
		`SELECT count()
		 FROM open_splunk.events
		 WHERE event_id = ?
		   AND tenant_id = ?
		   AND index_name = ?
		   AND visibility_seq = 1
		   AND field_metadata_version = 0
		   AND empty(field_types)`,
		markerEventID,
		markerTenant,
		markerIndex,
	).Scan(&markerCount); err != nil {
		t.Fatalf("query persistent Compose marker event and migrated columns: %v", err)
	}
	if markerCount != 1 {
		t.Fatalf(
			"persistent Compose marker count = %d, want 1",
			markerCount,
		)
	}
}

func expectComposePrincipalCredentialsRejected(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
	credentials deploymentComposeCredentials,
) {
	t.Helper()
	for _, principal := range []struct {
		name     string
		database string
		password string
	}{
		{
			name:     composeMigrationUsername,
			database: "default",
			password: credentials.migrationPassword,
		},
		{
			name:     composeRuntimeUsername,
			database: "open_splunk",
			password: credentials.runtimePassword,
		},
		{
			name:     composeDeletionUsername,
			database: "open_splunk",
			password: credentials.deletionPassword,
		},
		{
			name:     composeBackupUsername,
			database: "open_splunk",
			password: credentials.backupPassword,
		},
		{
			name:     composeRestoreUsername,
			database: "default",
			password: credentials.restorePassword,
		},
	} {
		connection := openDeploymentComposeConnection(
			t,
			address,
			principal.database,
			principal.name,
			principal.password,
			stack.clientTLSConfig(),
		)
		err := connection.Ping(ctx)
		closeErr := connection.Close()
		if err == nil {
			t.Fatalf("old %s credential unexpectedly connected", principal.name)
		}
		if !isClickHouseAuthenticationError(err) {
			t.Fatalf(
				"old %s credential error = %v, want authentication failure",
				principal.name,
				err,
			)
		}
		if closeErr != nil {
			t.Fatalf("close rejected old %s connection: %v", principal.name, closeErr)
		}
	}
}

func openDeploymentComposeConnection(
	t *testing.T,
	address string,
	database string,
	username string,
	password string,
	tlsConfig *tls.Config,
) clickhousedriver.Conn {
	t.Helper()
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{address},
		Auth: clickhousedriver.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		TLS:          tlsConfig,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		MaxOpenConns: 2,
		MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("open deployment ClickHouse principal %q: %v", username, err)
	}
	return connection
}

func closeDeploymentComposeConnection(
	t *testing.T,
	principal string,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	if err := connection.Close(); err != nil {
		t.Errorf("close deployment %s principal: %v", principal, err)
	}
}

func isClickHouseAuthenticationError(err error) bool {
	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) {
		return exception.Code == 516
	}
	return isClickHouseAuthenticationFailure(err.Error())
}

func isClickHouseAuthenticationFailure(output string) bool {
	lowerOutput := strings.ToLower(output)
	return strings.Contains(lowerOutput, "authentication failed") ||
		strings.Contains(lowerOutput, "password is incorrect")
}

func boundedComposeOutput(output []byte) string {
	const maximumBytes = 16 * 1024
	trimmed := strings.TrimSpace(string(output))
	if len(trimmed) <= maximumBytes {
		return trimmed
	}
	return fmt.Sprintf(
		"%s\n... %d bytes omitted",
		trimmed[len(trimmed)-maximumBytes:],
		len(trimmed)-maximumBytes,
	)
}
