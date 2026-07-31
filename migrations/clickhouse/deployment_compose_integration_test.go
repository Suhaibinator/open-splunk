package clickhouse_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	shippedmigrations "github.com/Suhaibinator/open-splunk/migrations"
)

const (
	composeBootstrapUsername = "open_splunk_bootstrap"
	composeMigrationUsername = "open_splunk_migrator"
	composeRuntimeUsername   = "open_splunk_runtime"
	composeDeletionUsername  = "open_splunk_deletion"
)

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
	if output, err := composeProbe.CombinedOutput(); err != nil {
		t.Fatalf(
			"Docker integration was requested but Compose is unavailable: %v: %s",
			err,
			boundedComposeOutput(output),
		)
	}

	deployDirectory := deploymentDirectory(t)
	credentials, tlsIdentity := generateDeploymentComposeEnvironment(
		t,
		deployDirectory,
	)
	stack := &deploymentComposeStack{
		project:          "open-splunk-compose-test-" + randomHex(t, 6),
		projectDirectory: deployDirectory,
		composeFile:      filepath.Join(deployDirectory, "docker-compose.yaml"),
		credentials:      credentials,
		tlsIdentity:      tlsIdentity,
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
	images := stack.mustRun(t, ctx, "resolve deployment image", "config", "--images")
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
	firstAddress := stack.publishedAddress(t, ctx, "9440")
	_ = stack.publishedAddress(t, ctx, "9000")
	_ = stack.publishedAddress(t, ctx, "8123")

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
	secondAddress := stack.publishedAddress(t, ctx, "9440")
	_ = stack.publishedAddress(t, ctx, "9000")
	_ = stack.publishedAddress(t, ctx, "8123")

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
}

type deploymentComposeCredentials struct {
	bootstrapPassword string
	migrationPassword string
	runtimePassword   string
	deletionPassword  string
}

type deploymentComposeTLSIdentity struct {
	caCertificateFile string
	certificateFile   string
	privateKeyFile    string
	serverName        string
	rootCAs           *x509.CertPool
}

func generateDeploymentComposeEnvironment(
	t *testing.T,
	deployDirectory string,
) (deploymentComposeCredentials, *deploymentComposeTLSIdentity) {
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
	}
	for name, password := range map[string]string{
		"bootstrap": credentials.bootstrapPassword,
		"migration": credentials.migrationPassword,
		"runtime":   credentials.runtimePassword,
		"deletion":  credentials.deletionPassword,
	} {
		if len(password) != 64 || strings.Trim(password, "0123456789abcdef") != "" {
			t.Fatalf("generated deployment %s password is invalid", name)
		}
	}
	return credentials, &deploymentComposeTLSIdentity{
		caCertificateFile: caCertificateFile,
		certificateFile:   values["OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE"],
		privateKeyFile:    values["OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE"],
		serverName:        values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		rootCAs:           roots,
	}
}

func newDeploymentComposeCredentials(
	t *testing.T,
	excluded map[string]struct{},
) deploymentComposeCredentials {
	t.Helper()
	used := make(map[string]struct{}, len(excluded)+4)
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
	}
}

func (credentials deploymentComposeCredentials) passwordSet() map[string]struct{} {
	return map[string]struct{}{
		credentials.bootstrapPassword: {},
		credentials.migrationPassword: {},
		credentials.runtimePassword:   {},
		credentials.deletionPassword:  {},
	}
}

type deploymentComposeStack struct {
	project          string
	projectDirectory string
	composeFile      string
	credentials      deploymentComposeCredentials
	tlsIdentity      *deploymentComposeTLSIdentity
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
	composeArguments = append(composeArguments, arguments...)
	command := exec.CommandContext(ctx, "docker", composeArguments...)
	command.Env = stack.environment(additionalEnvironment)
	command.WaitDelay = 5 * time.Second
	return command.CombinedOutput()
}

func (stack *deploymentComposeStack) environment(
	additionalEnvironment map[string]string,
) []string {
	values := map[string]string{
		"COMPOSE_ANSI": "never",
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD": stack.credentials.bootstrapPassword,
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD": stack.credentials.migrationPassword,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD":   stack.credentials.runtimePassword,
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD":  stack.credentials.deletionPassword,
		"OPEN_SPLUNK_CLICKHOUSE_HTTP_PORT":          "0",
		"OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT":        "0",
		"OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT": "0",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE":        stack.tlsIdentity.caCertificateFile,
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE":      stack.tlsIdentity.certificateFile,
		"OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE":       stack.tlsIdentity.privateKeyFile,
		"OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME":    stack.tlsIdentity.serverName,
	}
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
	containerPort string,
) string {
	t.Helper()
	output := stack.mustRun(
		t,
		ctx,
		"resolve published ClickHouse port "+containerPort,
		"port",
		"clickhouse",
		containerPort,
	)
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
	if len(images) != 1 {
		t.Fatalf("Docker Compose images = %q, want exactly one ClickHouse image", output)
	}
	image := images[0]
	digestIndex := strings.LastIndex(image, "@sha256:")
	if digestIndex <= 0 ||
		len(image[digestIndex+len("@sha256:"):]) != 64 ||
		strings.Trim(image[digestIndex+len("@sha256:"):], "0123456789abcdef") != "" {
		t.Fatalf(
			"deploy/docker-compose.yaml image must be digest-pinned, got %q",
			image,
		)
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
	output, err := command.CombinedOutput()
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

func verifyComposeBootstrapBoundary(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	address string,
) {
	t.Helper()
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

	const query = `clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"`
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
		state.ServerName != stack.tlsIdentity.serverName {
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
		stack.tlsIdentity.serverName,
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
		RootCAs:    stack.tlsIdentity.rootCAs.Clone(),
		ServerName: stack.tlsIdentity.serverName,
	}
}

func expectComposeBootstrapPasswordRejected(
	t *testing.T,
	ctx context.Context,
	stack *deploymentComposeStack,
	oldPassword string,
) {
	t.Helper()
	const passwordEnvironment = "OPEN_SPLUNK_CLICKHOUSE_OLD_PASSWORD_PROBE"
	const query = `clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$OPEN_SPLUNK_CLICKHOUSE_OLD_PASSWORD_PROBE" --query "SELECT 1"`
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

	const (
		markerTenant = "compose-persistence-tenant"
		markerIndex  = "compose-persistence-index"
	)
	if insertMarker {
		if err := runtimeConnection.Exec(
			ctx,
			`INSERT INTO open_splunk.events
				(
					event_id, tenant_id, index_name, event_time, index_time,
					expires_at, visibility_seq
				)
			 VALUES (?, ?, ?, now64(9), now64(3), now64(3) + INTERVAL 1 DAY, 1)`,
			markerEventID,
			markerTenant,
			markerIndex,
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
