package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestDevelopmentWorkflow exercises the documented host-server/containerized-
// ClickHouse path. It is opt-in because it builds the UI and starts Docker.
func TestDevelopmentWorkflow(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_DEVELOPMENT_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_DEVELOPMENT_INTEGRATION=1 to run the development workflow smoke test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the development shell workflow is supported on Unix hosts")
	}

	repositoryRoot := integrationRepositoryRoot(t)
	requireCommand(t, "docker")
	requireCommand(t, "make")
	clickHousePort := reserveLoopbackPort(t)
	httpPort := reserveLoopbackPort(t)
	temporaryRoot := t.TempDir()
	environmentFile := filepath.Join(temporaryRoot, ".env.development")

	runDevelopmentCommand(t, repositoryRoot,
		filepath.Join(repositoryRoot, "deploy", "generate-env.sh"),
		"--development", environmentFile,
	)
	replaceDevelopmentPort(t, environmentFile, "OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT", clickHousePort)
	replaceDevelopmentValue(
		t,
		environmentFile,
		"OPEN_SPLUNK_CLICKHOUSE_ADDRESS",
		"127.0.0.1:"+strconv.Itoa(clickHousePort),
	)
	replaceDevelopmentPort(t, environmentFile, "OPEN_SPLUNK_SERVER_HTTP_PORT", httpPort)

	project := "open-splunk-development-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	composeArguments := []string{
		"compose", "--project-name", project,
		"--env-file", environmentFile,
		"-f", filepath.Join(repositoryRoot, "deploy", "docker-compose.development.yaml"),
	}
	t.Cleanup(func() {
		arguments := append(append([]string{}, composeArguments...), "down", "--volumes", "--remove-orphans")
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(cleanupContext, "docker", arguments...)
		command.Dir = repositoryRoot
		if output, err := command.CombinedOutput(); err != nil {
			t.Logf("development Compose cleanup: %v\n%s", err, output)
		}
	})
	runDevelopmentCommand(t, repositoryRoot, "docker",
		append(composeArguments, "up", "--detach", "--wait", "clickhouse")...,
	)
	runDevelopmentCommand(t, repositoryRoot, "make", "build-server")

	serverBinary := filepath.Join(repositoryRoot, "build", "open-splunk-server")
	identity := runDevelopmentCommand(t, repositoryRoot, serverBinary, "version")
	if identity != "source_revision=development\n" {
		t.Fatalf("development identity = %q", identity)
	}

	stateRoot := filepath.Join(temporaryRoot, "state")
	exportRoot := filepath.Join(temporaryRoot, "exports")
	serverEnvironment := []string{
		"OPEN_SPLUNK_DEVELOPMENT_STATE_ROOT=" + stateRoot,
		"OPEN_SPLUNK_DEVELOPMENT_EXPORT_ROOT=" + exportRoot,
		"OPEN_SPLUNK_DEVELOPMENT_SERVER_BINARY=" + serverBinary,
	}
	logPath := filepath.Join(temporaryRoot, "server.log")
	process := startDevelopmentServer(t, repositoryRoot, environmentFile, serverEnvironment, logPath)
	waitForDevelopmentReadiness(t, httpPort, logPath)
	stopDevelopmentServer(t, process, logPath)

	masterKeyPath := filepath.Join(stateRoot, "private", "master.key")
	controlDatabasePath := filepath.Join(stateRoot, "private", "open-splunk.db")
	masterKey := readNonemptyFile(t, masterKeyPath)
	if info, err := os.Stat(controlDatabasePath); err != nil || info.Size() == 0 {
		t.Fatalf("development control database was not retained: info=%v error=%v", info, err)
	}

	process = startDevelopmentServer(t, repositoryRoot, environmentFile, serverEnvironment, logPath)
	waitForDevelopmentReadiness(t, httpPort, logPath)
	stopDevelopmentServer(t, process, logPath)
	if restartedMasterKey := readNonemptyFile(t, masterKeyPath); !bytes.Equal(restartedMasterKey, masterKey) {
		t.Fatal("development restart replaced the persisted master key")
	}
}

func integrationRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), ".."))
}

func requireCommand(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Fatalf("%s is required: %v", name, err)
	}
}

func reserveLoopbackPort(t *testing.T) int {
	t.Helper()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func replaceDevelopmentPort(t *testing.T, path, name string, port int) {
	t.Helper()
	replaceDevelopmentValue(t, path, name, strconv.Itoa(port))
}

func replaceDevelopmentValue(t *testing.T, path, name, value string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read development environment: %v", err)
	}
	prefix := name + "="
	lines := strings.Split(string(contents), "\n")
	replaced := false
	for index, line := range lines {
		if strings.HasPrefix(line, prefix) {
			lines[index] = prefix + value
			replaced = true
		}
	}
	if !replaced {
		t.Fatalf("development environment has no %s", name)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write development environment: %v", err)
	}
}

func runDevelopmentCommand(t *testing.T, directory string, name string, arguments ...string) string {
	t.Helper()
	command := exec.CommandContext(t.Context(), name, arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run %s: %v\n%s", name, err, output)
	}
	return string(output)
}

func startDevelopmentServer(t *testing.T, repositoryRoot, environmentFile string, environment []string, logPath string) *exec.Cmd {
	t.Helper()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open development server log: %v", err)
	}
	command := exec.CommandContext(
		t.Context(),
		filepath.Join(repositoryRoot, "scripts", "run-development.sh"),
		environmentFile,
	)
	command.Dir = repositoryRoot
	command.Env = append(os.Environ(), environment...)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start development server: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		_ = logFile.Close()
	})
	return command
}

func waitForDevelopmentReadiness(t *testing.T, port int, logPath string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("create development readiness request: %v", err)
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("development server did not become ready; log:\n%s", readFileForFailure(logPath))
}

func stopDevelopmentServer(t *testing.T, process *exec.Cmd, logPath string) {
	t.Helper()
	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("stop development server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("development server exit: %v; log:\n%s", err, readFileForFailure(logPath))
		}
	case <-time.After(30 * time.Second):
		_ = process.Process.Kill()
		<-done
		t.Fatalf("development server did not stop after SIGTERM; log:\n%s", readFileForFailure(logPath))
	}
}

func readNonemptyFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read retained development file %s: %v", path, err)
	}
	if len(contents) == 0 {
		t.Fatalf("retained development file is empty: %s", path)
	}
	return contents
}

func readFileForFailure(path string) string {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(contents)
}
