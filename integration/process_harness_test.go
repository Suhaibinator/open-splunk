//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := cleanupBackendFrontendStage(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "clean backend frontend integration stage: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func postProto(t *testing.T, ctx context.Context, client *http.Client, url string, input, output proto.Message) []byte {
	t.Helper()
	body, err := postProtoRequest(ctx, client, url, input, output)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func postProtoRequest(
	ctx context.Context,
	client *http.Client,
	url string,
	input, output proto.Message,
) ([]byte, error) {
	payload, err := proto.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode POST %s: %w", url, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create POST %s: %w", url, err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read POST %s: %w", url, err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST %s status = %d, body = %q", url, response.StatusCode, body)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/x-protobuf" {
		return nil, fmt.Errorf("POST %s content type = %q", url, contentType)
	}
	if err := proto.Unmarshal(body, output); err != nil {
		return nil, fmt.Errorf("decode POST %s: %w", url, err)
	}
	return body, nil
}

func TestRedactForFailure(t *testing.T) {
	const secret = "protected-value"
	redacted := redactForFailure("before "+secret+" after "+secret, secret)
	if strings.Contains(redacted, secret) || redacted != "before [REDACTED] after [REDACTED]" {
		t.Fatalf("redactForFailure() = %q", redacted)
	}
}

func TestEnvironmentWithValueReplacesEveryExistingValue(t *testing.T) {
	got := environmentWithValue([]string{"PATH=/bin", "MODE=demo", "OTHER=value", "MODE=stale"}, "MODE", "backend")
	want := []string{"PATH=/bin", "OTHER=value", "MODE=backend"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("environmentWithValue() = %q, want %q", got, want)
	}
}

func TestCollectorWALAdvancedRequiresNewOrGrowingDurableFile(t *testing.T) {
	t.Parallel()

	before := map[string]int64{"segment-1.wal": 128}
	for _, test := range []struct {
		name  string
		after map[string]int64
		want  bool
	}{
		{name: "unchanged", after: map[string]int64{"segment-1.wal": 128}},
		{name: "only shrank", after: map[string]int64{"segment-1.wal": 100}},
		{name: "grew", after: map[string]int64{"segment-1.wal": 129}, want: true},
		{name: "new empty", after: map[string]int64{"segment-2.wal": 0}},
		{name: "new durable file", after: map[string]int64{"segment-2.wal": 1}, want: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := collectorWALAdvanced(before, test.after); got != test.want {
				t.Fatalf("collectorWALAdvanced() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestManagedProcessWaitDistinguishesExitAndTimeout(t *testing.T) {
	t.Parallel()
	exitErr := errors.New("process failed")
	for _, test := range []struct {
		name    string
		process *managedProcess
		timeout time.Duration
		want    error
	}{
		{
			name:    "clean exit",
			process: completedManagedProcess(nil),
			timeout: time.Second,
		},
		{
			name:    "failed exit",
			process: completedManagedProcess(exitErr),
			timeout: time.Second,
			want:    exitErr,
		},
		{
			name:    "timeout",
			process: &managedProcess{done: make(chan struct{})},
			timeout: time.Millisecond,
			want:    errManagedProcessWaitTimeout,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.process.Wait(test.timeout); !errors.Is(err, test.want) {
				t.Fatalf("Wait() = %v, want %v", err, test.want)
			}
		})
	}
}

func TestLockedBufferReportsTruncation(t *testing.T) {
	t.Parallel()
	buffer := &lockedBuffer{maximum: 4}
	if written, err := buffer.Write([]byte("abcdef")); err != nil || written != 6 {
		t.Fatalf("Write() = %d, %v", written, err)
	}
	if got := buffer.String(); got != "abcd" {
		t.Fatalf("String() = %q, want %q", got, "abcd")
	}
	if !buffer.Truncated() {
		t.Fatal("Truncated() = false, want true")
	}
}

func TestRunCommandWithBoundedOutputCapsCombinedStreams(t *testing.T) {
	t.Parallel()

	command := exec.Command(os.Args[0], "-test.run=^TestHarnessOutputEmitter$")
	command.Env = environmentWithValue(
		os.Environ(),
		"OPEN_SPLUNK_TEST_HARNESS_OUTPUT_EMITTER",
		"overflow",
	)
	const maximum = 1 << 10
	output, truncated, err := runCommandWithBoundedOutput(command, maximum)
	if err == nil {
		t.Fatal("runCommandWithBoundedOutput() error = nil, want helper-process failure")
	}
	if !truncated {
		t.Fatal("runCommandWithBoundedOutput() truncated = false, want true")
	}
	if len(output) != maximum {
		t.Fatalf("len(output) = %d, want %d", len(output), maximum)
	}
	if strings.Trim(output, "oe") != "" {
		t.Fatalf("output contains bytes other than the emitted stdout/stderr payload: %q", output)
	}
}

func TestRunCommandWithBoundedOutputBoundaries(t *testing.T) {
	t.Parallel()

	const maximum = 1 << 10
	for _, test := range []struct {
		name          string
		mode          string
		wantError     bool
		wantLength    int
		wantTruncated bool
	}{
		{name: "success", mode: "success", wantLength: len("stdoutstderr")},
		{name: "failure", mode: "failure", wantError: true, wantLength: len("stdoutstderr")},
		{name: "exact limit", mode: "exact", wantLength: maximum},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := exec.Command(os.Args[0], "-test.run=^TestHarnessOutputEmitter$")
			command.Env = environmentWithValue(
				os.Environ(),
				"OPEN_SPLUNK_TEST_HARNESS_OUTPUT_EMITTER",
				test.mode,
			)
			output, truncated, err := runCommandWithBoundedOutput(command, maximum)
			if (err != nil) != test.wantError {
				t.Fatalf("runCommandWithBoundedOutput() error = %v, want error %t", err, test.wantError)
			}
			if truncated != test.wantTruncated {
				t.Fatalf(
					"runCommandWithBoundedOutput() truncated = %t, want %t",
					truncated,
					test.wantTruncated,
				)
			}
			if len(output) != test.wantLength {
				t.Fatalf("len(output) = %d, want %d", len(output), test.wantLength)
			}
			if test.mode == "success" || test.mode == "failure" {
				if !strings.Contains(output, "stdout") || !strings.Contains(output, "stderr") {
					t.Fatalf("output = %q, want both streams", output)
				}
			}
		})
	}

	t.Run("start failure", func(t *testing.T) {
		t.Parallel()
		command := exec.Command(filepath.Join(t.TempDir(), "missing-command"))
		output, truncated, err := runCommandWithBoundedOutput(command, maximum)
		if err == nil {
			t.Fatal("runCommandWithBoundedOutput() error = nil, want start failure")
		}
		if output != "" || truncated {
			t.Fatalf(
				"runCommandWithBoundedOutput() = %q, truncated %t, want empty and complete",
				output,
				truncated,
			)
		}
	})
}

func TestFormatBoundedCommandOutputIncludesMarkerWithinLimit(t *testing.T) {
	t.Parallel()

	const maximum = 64
	formatted := formatBoundedCommandOutput(strings.Repeat("x", maximum), true, maximum)
	if len(formatted) != maximum {
		t.Fatalf("len(formatBoundedCommandOutput()) = %d, want %d", len(formatted), maximum)
	}
	if !strings.HasSuffix(formatted, commandOutputTruncatedSuffix) {
		t.Fatalf("formatBoundedCommandOutput() = %q, want truncation suffix", formatted)
	}
}

func TestHarnessOutputEmitter(t *testing.T) {
	switch os.Getenv("OPEN_SPLUNK_TEST_HARNESS_OUTPUT_EMITTER") {
	case "":
		return
	case "success":
		_, _ = os.Stdout.WriteString("stdout")
		_, _ = os.Stderr.WriteString("stderr")
		os.Exit(0)
	case "failure":
		_, _ = os.Stdout.WriteString("stdout")
		_, _ = os.Stderr.WriteString("stderr")
		os.Exit(23)
	case "exact":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 1<<10))
		os.Exit(0)
	case "overflow":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("o"), 2<<10))
		_, _ = os.Stderr.Write(bytes.Repeat([]byte("e"), 2<<10))
		os.Exit(23)
	default:
		t.Fatalf("unknown harness output emitter mode")
	}
}

func completedManagedProcess(err error) *managedProcess {
	process := &managedProcess{done: make(chan struct{}), err: err}
	close(process.done)
	return process
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if info, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil && !info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not locate repository go.mod")
		}
		directory = parent
	}
}

func buildBinary(t *testing.T, ctx context.Context, repository, output, pkg string) {
	t.Helper()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", output, pkg)
	configureProcessGroup(command)
	command.Dir = repository
	command.Env = environmentWithValue(os.Environ(), "CGO_ENABLED", "0")
	combined, truncated, err := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if err != nil {
		t.Fatalf(
			"build %s: %v\n%s",
			pkg,
			err,
			formatBoundedCommandOutput(combined, truncated, maximumHarnessOutputBytes),
		)
	}
	if truncated {
		t.Fatalf("build %s produced more than %d bytes of output", pkg, maximumHarnessOutputBytes)
	}
}

const (
	backendFrontendStagePrefix   = "open-splunk-backend-frontend-"
	maximumHarnessOutputBytes    = 1 << 20
	commandOutputTruncatedSuffix = "\n... [command output truncated]"
)

var backendFrontendBuild struct {
	once       sync.Once
	sourceRoot string
	stageRoot  string
	err        error
	logs       string
}

var backendFrontendStageRootFiles = []string{
	"go.mod",
	"go.sum",
	"next-env.d.ts",
	"next.config.ts",
	"package-lock.json",
	"package.json",
	"tsconfig.json",
	"webui.go",
}

var backendFrontendStageDirectories = []string{
	"app",
	filepath.Join("cmd", "open-splunk-server"),
	filepath.Join("gen", "go"),
	filepath.Join("gen", "ts"),
	"internal",
	"lib",
	"migrations",
	"public",
}

func buildBackendFrontend(t *testing.T, ctx context.Context, repository string) string {
	t.Helper()
	sourceRoot, err := canonicalDirectory(repository)
	if err != nil {
		t.Fatalf("resolve backend frontend source repository: %v", err)
	}
	backendFrontendBuild.once.Do(func() {
		backendFrontendBuild.sourceRoot = sourceRoot
		backendFrontendBuild.stageRoot, backendFrontendBuild.logs, backendFrontendBuild.err =
			stageBackendFrontend(ctx, sourceRoot)
	})
	if backendFrontendBuild.sourceRoot != sourceRoot {
		t.Fatalf(
			"backend frontend was staged from %q and cannot be reused for %q",
			backendFrontendBuild.sourceRoot,
			sourceRoot,
		)
	}
	if backendFrontendBuild.err != nil {
		t.Fatalf("build isolated backend frontend: %v\n%s", backendFrontendBuild.err, backendFrontendBuild.logs)
	}
	return backendFrontendBuild.stageRoot
}

func stageBackendFrontend(ctx context.Context, sourceRoot string) (string, string, error) {
	if ctx == nil {
		return "", "", errors.New("backend frontend build context is required")
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	stageRoot, err := os.MkdirTemp(os.TempDir(), backendFrontendStagePrefix)
	if err != nil {
		return "", "", fmt.Errorf("create isolated repository: %w", err)
	}
	if err := copyBackendFrontendStageSources(ctx, sourceRoot, stageRoot); err != nil {
		return stageRoot, "", err
	}
	nodeModules, err := canonicalDirectory(filepath.Join(sourceRoot, "node_modules"))
	if err != nil {
		return stageRoot, "", fmt.Errorf("resolve frontend dependencies: %w", err)
	}
	if err := os.Symlink(nodeModules, filepath.Join(stageRoot, "node_modules")); err != nil {
		return stageRoot, "", fmt.Errorf("link frontend dependencies: %w", err)
	}

	// Turbopack rejects node_modules symlinks whose targets are outside its
	// project root. The isolated stage intentionally uses such a symlink so it
	// can reuse immutable installed dependencies without copying hundreds of
	// megabytes. Webpack follows the same production Next build path without
	// imposing that filesystem-root restriction.
	command := exec.CommandContext(ctx, "npm", "run", "build", "--", "--webpack")
	configureProcessGroup(command)
	command.Dir = stageRoot
	command.Env = environmentWithValue(os.Environ(), "OPEN_SPLUNK_DATA_MODE", "backend")
	command.Env = environmentWithValue(command.Env, "OPEN_SPLUNK_API_BASE_URL", "")
	command.Env = environmentWithValue(command.Env, "NEXT_TELEMETRY_DISABLED", "1")
	logs, truncated, runErr := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if runErr != nil {
		return stageRoot,
			formatBoundedCommandOutput(logs, truncated, maximumHarnessOutputBytes),
			fmt.Errorf("build staged frontend: %w", runErr)
	}
	if truncated {
		return stageRoot, "", fmt.Errorf(
			"build staged frontend output exceeded %d bytes",
			maximumHarnessOutputBytes,
		)
	}
	if _, err := os.Stat(filepath.Join(stageRoot, "out", "search", "index.html")); err != nil {
		return stageRoot, logs, fmt.Errorf("staged backend frontend export is incomplete: %w", err)
	}
	return stageRoot, logs, nil
}

func copyBackendFrontendStageSources(ctx context.Context, sourceRoot, stageRoot string) error {
	for _, relativePath := range backendFrontendStageRootFiles {
		if err := copyStageFile(
			ctx,
			filepath.Join(sourceRoot, relativePath),
			filepath.Join(stageRoot, relativePath),
		); err != nil {
			return fmt.Errorf("stage %q: %w", relativePath, err)
		}
	}
	for _, relativePath := range backendFrontendStageDirectories {
		if err := copyStageTree(
			ctx,
			filepath.Join(sourceRoot, relativePath),
			filepath.Join(stageRoot, relativePath),
		); err != nil {
			return fmt.Errorf("stage %q: %w", relativePath, err)
		}
	}
	return nil
}

func copyStageTree(
	ctx context.Context,
	sourceRoot string,
	destinationRoot string,
) error {
	return filepath.WalkDir(sourceRoot, func(sourcePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relativePath, err := filepath.Rel(sourceRoot, sourcePath)
		if err != nil {
			return err
		}
		if relativePath == "." {
			return os.MkdirAll(destinationRoot, 0o755)
		}
		destinationPath := filepath.Join(destinationRoot, relativePath)
		if entry.IsDir() {
			return os.MkdirAll(destinationPath, 0o755)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("source %q is a symbolic link", sourcePath)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("source %q is not a regular file", sourcePath)
		}
		return copyStageFile(ctx, sourcePath, destinationPath)
	})
}

func copyStageFile(ctx context.Context, sourcePath, destinationPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	sourceInfo, err := source.Stat()
	if err != nil {
		_ = source.Close()
		return err
	}
	if !sourceInfo.Mode().IsRegular() {
		_ = source.Close()
		return fmt.Errorf("source is not a regular file")
	}
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		_ = source.Close()
		return err
	}
	destination, err := os.OpenFile(
		destinationPath,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		sourceInfo.Mode().Perm(),
	)
	if err != nil {
		_ = source.Close()
		return err
	}
	reader := &contextReader{ctx: ctx, reader: source}
	_, copyErr := io.CopyBuffer(destination, reader, make([]byte, 128<<10))
	closeDestinationErr := destination.Close()
	closeSourceErr := source.Close()
	if err := errors.Join(copyErr, closeDestinationErr, closeSourceErr); err != nil {
		_ = os.Remove(destinationPath)
		return err
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
	}
	return reader.reader.Read(buffer)
}

func canonicalDirectory(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%q is not a directory", path)
	}
	return canonical, nil
}

func cleanupBackendFrontendStage() error {
	stageRoot := backendFrontendBuild.stageRoot
	if stageRoot == "" {
		return nil
	}
	temporaryRoot, err := filepath.Abs(os.TempDir())
	if err != nil {
		return fmt.Errorf("resolve temporary directory: %w", err)
	}
	stageRoot, err = filepath.Abs(stageRoot)
	if err != nil {
		return fmt.Errorf("resolve stage directory: %w", err)
	}
	if filepath.Dir(stageRoot) != temporaryRoot ||
		!strings.HasPrefix(filepath.Base(stageRoot), backendFrontendStagePrefix) {
		return fmt.Errorf("refuse to remove unsafe stage path %q", stageRoot)
	}
	if err := os.RemoveAll(stageRoot); err != nil {
		return fmt.Errorf("remove %q: %w", stageRoot, err)
	}
	backendFrontendBuild.stageRoot = ""
	return nil
}

func environmentWithValue(environment []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}

var browserE2EModeFlags = []string{
	"OPEN_SPLUNK_E2E_CANCELLATION_TEST",
	"OPEN_SPLUNK_E2E_RENDERING_TEST",
	"OPEN_SPLUNK_E2E_SEQUENCE_EXPIRATION_TEST",
	"OPEN_SPLUNK_E2E_SEQUENCE_GAP_TEST",
	"OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_FIRST_PROGRESS_TEST",
	"OPEN_SPLUNK_E2E_SEQUENCE_GAP_REST_TERMINAL_TEST",
}

type browserVerticalSpecConfig struct {
	grepPattern        string
	outputDirectory    string
	failureDescription string
	environment        map[string]string
}

func runBrowserVerticalSpec(
	t *testing.T,
	ctx context.Context,
	repository string,
	config browserVerticalSpecConfig,
) {
	t.Helper()
	browserContext, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	command := exec.CommandContext(
		browserContext,
		filepath.Join(repository, "node_modules", ".bin", "playwright"),
		"test",
		"integration/browser_vertical.spec.ts",
		"--workers=1",
		"--reporter=line",
		"--grep="+config.grepPattern,
		"--output="+filepath.Join(repository, "test-results", config.outputDirectory),
	)
	configureProcessGroup(command)
	command.Dir = repository
	environment := os.Environ()
	for _, flag := range browserE2EModeFlags {
		environment = environmentWithValue(environment, flag, "0")
	}
	for name, value := range config.environment {
		environment = environmentWithValue(environment, name, value)
	}
	command.Env = environment
	logs, truncated, runErr := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	if runErr != nil {
		t.Fatalf(
			"%s: %v\n%s",
			config.failureDescription,
			runErr,
			formatBoundedCommandOutput(logs, truncated, maximumHarnessOutputBytes),
		)
	}
	if truncated {
		t.Fatalf("%s logs exceeded %d bytes", config.failureDescription, maximumHarnessOutputBytes)
	}
}

func configureProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		// Playwright handles SIGTERM by closing its separately grouped browser
		// process. A hard kill here would bypass that cleanup and orphan Chrome.
		err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 5 * time.Second
}

func unusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func redactForFailure(value string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

type managedProcess struct {
	command *exec.Cmd
	logs    *lockedBuffer
	done    chan struct{}

	mu  sync.Mutex
	err error
}

var errManagedProcessWaitTimeout = errors.New("managed process wait timed out")

func startProcess(t *testing.T, directory string, arguments []string, environment []string) *managedProcess {
	t.Helper()
	if len(arguments) == 0 {
		t.Fatal("process command is required")
	}
	logs := &lockedBuffer{maximum: maximumHarnessOutputBytes}
	command := exec.Command(arguments[0], arguments[1:]...)
	command.Dir = directory
	command.Env = environment
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", arguments[0], err)
	}
	process := &managedProcess{command: command, logs: logs, done: make(chan struct{})}
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		if err := process.Kill(5 * time.Second); err != nil {
			t.Errorf("force process cleanup: %v", err)
		}
	})
	return process
}

func (process *managedProcess) Interrupt(timeout time.Duration) error {
	select {
	case <-process.done:
		return process.Err()
	default:
	}
	if err := process.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		return process.Err()
	case <-timer.C:
		killErr := process.Kill(5 * time.Second)
		return fmt.Errorf("graceful shutdown timed out after %s (force cleanup: %v)", timeout, killErr)
	}
}

func (process *managedProcess) Wait(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("%w: timeout must be positive", errManagedProcessWaitTimeout)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		return process.Err()
	case <-timer.C:
		return fmt.Errorf("%w after %s", errManagedProcessWaitTimeout, timeout)
	}
}

func (process *managedProcess) Kill(timeout time.Duration) error {
	select {
	case <-process.done:
		return nil
	default:
	}
	if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("process did not exit within %s after kill", timeout)
	}
}

func (process *managedProcess) Exited() bool {
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *managedProcess) Err() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.err
}

func (process *managedProcess) Logs() string { return process.logs.String() }

func assertManagedProcessLogsComplete(
	t *testing.T,
	name string,
	process *managedProcess,
	secrets ...string,
) {
	t.Helper()
	if process.logs.Truncated() {
		t.Fatalf(
			"%s logs exceeded the in-memory capture limit; captured prefix:\n%s",
			name,
			redactForFailure(process.Logs(), secrets...),
		)
	}
}

type lockedBuffer struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	maximum   int
	truncated bool
}

func runCommandWithBoundedOutput(command *exec.Cmd, maximum int) (string, bool, error) {
	logs := &lockedBuffer{maximum: maximum}
	command.Stdout = logs
	command.Stderr = logs
	err := command.Run()
	output, truncated := logs.Snapshot()
	return output, truncated, err
}

func formatBoundedCommandOutput(output string, truncated bool, maximum int) string {
	if !truncated {
		return output
	}
	if maximum <= len(commandOutputTruncatedSuffix) {
		return commandOutputTruncatedSuffix[len(commandOutputTruncatedSuffix)-maximum:]
	}
	prefixLength := min(len(output), maximum-len(commandOutputTruncatedSuffix))
	return output[:prefixLength] + commandOutputTruncatedSuffix
}

func (buffer *lockedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	written := len(value)
	remaining := buffer.maximum - buffer.buffer.Len()
	if remaining > 0 {
		_, _ = buffer.buffer.Write(value[:min(len(value), remaining)])
	}
	if len(value) > max(remaining, 0) {
		buffer.truncated = true
	}
	return written, nil
}

func (buffer *lockedBuffer) String() string {
	value, _ := buffer.Snapshot()
	return value
}

func (buffer *lockedBuffer) Snapshot() (string, bool) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String(), buffer.truncated
}

func (buffer *lockedBuffer) Truncated() bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.truncated
}
