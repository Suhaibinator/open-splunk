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
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/durationpb"
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

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
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
	bootstrapID := stack.serviceContainerID(t, ctx, "server-bootstrap", true)
	serverContainer := releaseOCIInspectContainer(t, ctx, stack, serverID)
	clickHouseContainer := releaseOCIInspectContainer(t, ctx, stack, clickHouseID)
	migratorContainer := releaseOCIInspectContainer(t, ctx, stack, migratorID)
	bootstrapContainer := releaseOCIInspectContainer(t, ctx, stack, bootstrapID)
	releaseOCIAssertContainerHardening(t, serverContainer, "65532:65532", true)
	releaseOCIAssertContainerHardening(t, migratorContainer, "65532:65532", false)
	releaseOCIAssertContainerHardening(t, bootstrapContainer, "65532:65532", false)
	releaseOCIAssertContainerHardening(t, clickHouseContainer, "101:101", true)
	releaseOCIAssertContainerHealth(t, serverContainer, "server")
	releaseOCIAssertContainerHealth(t, clickHouseContainer, "clickhouse")
	releaseOCIAssertMigratorCompleted(t, migratorContainer)
	releaseOCIAssertMigratorIsolation(t, stack, migratorContainer)
	releaseOCIAssertBootstrapCompleted(t, bootstrapContainer)
	releaseOCIAssertBootstrapNetworkDisabled(t, bootstrapContainer)
	releaseOCIAssertClickHouseHasNoHostPorts(t, clickHouseContainer)
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
	recreatedBootstrapContainer := releaseOCIInspectContainer(t, ctx, stack, recreatedBootstrapID)
	if got := releaseOCIStateVolume(t, recreatedServerContainer); got != firstStateVolume {
		t.Fatalf("server state volume after force-recreate = %q, want %q", got, firstStateVolume)
	}
	releaseOCIAssertBootstrapCompleted(t, recreatedBootstrapContainer)
	releaseOCIAssertBootstrapNetworkDisabled(t, recreatedBootstrapContainer)
	releaseOCIAssertContainerHealth(t, recreatedClickHouseContainer, "recreated ClickHouse")
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
	project         string
	repository      string
	deployDirectory string
	composeFile     string
	envFile         string
	values          map[string]string
	retainedSecrets []string
	serverImage     string
	collectorImage  string
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
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE",
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
			Image string          `json:"image"`
			Ports []composePort   `json:"ports"`
			Build json.RawMessage `json:"build"`
		} `json:"services"`
	}
	if err := json.Unmarshal([]byte(output), &config); err != nil {
		t.Fatalf("decode production Compose config: %v", err)
	}
	wantServices := []string{
		"clickhouse",
		"clickhouse-migrator",
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
		config.Services["server-bootstrap"].Image != stack.serverImage {
		t.Fatalf("production server images do not use %q", stack.serverImage)
	}
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
	clickHouse := config.Services["clickhouse"]
	if clickHouse.Image != testsupport.DefaultClickHouseImage {
		t.Fatalf("production ClickHouse image = %q, want %q", clickHouse.Image, testsupport.DefaultClickHouseImage)
	}
	if len(clickHouse.Ports) != 0 {
		t.Fatalf("production ClickHouse publishes host ports: %+v", clickHouse.Ports)
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
		User       string   `json:"User"`
		Env        []string `json:"Env"`
		Entrypoint []string `json:"Entrypoint"`
		Cmd        []string `json:"Cmd"`
	} `json:"Config"`
	HostConfig struct {
		ReadonlyRootfs bool                  `json:"ReadonlyRootfs"`
		NetworkMode    string                `json:"NetworkMode"`
		PidsLimit      int64                 `json:"PidsLimit"`
		CapAdd         []string              `json:"CapAdd"`
		CapDrop        []string              `json:"CapDrop"`
		SecurityOpt    []string              `json:"SecurityOpt"`
		PortBindings   map[string][]struct{} `json:"PortBindings"`
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
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN":               {},
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE_CONTENTS": {},
	}
	secrets := stack.secrets()
	for _, entry := range container.Config.Env {
		name, value, _ := strings.Cut(entry, "=")
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
	joinedProcess := strings.Join(append(slices.Clone(container.Config.Entrypoint), container.Config.Cmd...), "\x00")
	if !slices.Contains(container.Config.Cmd, "-clickhouse-skip-migrations") ||
		strings.Contains(joinedProcess, "migration.password") {
		t.Fatalf("server process does not isolate migration credentials: %q", joinedProcess)
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
	for _, mount := range container.Mounts {
		if mount.Source == administratorSource || strings.Contains(mount.Destination, "open-splunk-bootstrap") {
			t.Fatalf("long-running server mounts administrator token source: %+v", mount)
		}
		if _, required := passwordFileMounts[mount.Destination]; required {
			if mount.RW {
				t.Fatalf("server ClickHouse password file mount is writable: %+v", mount)
			}
			passwordFileMounts[mount.Destination] = true
		}
		if strings.Contains(mount.Destination, "migration.password") {
			t.Fatalf("long-running server mounts migration credentials: %+v", mount)
		}
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
