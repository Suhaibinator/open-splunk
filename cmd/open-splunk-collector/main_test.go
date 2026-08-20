package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/protocolid"
)

// writeValidConfig writes a minimal valid collector config referencing a real
// token file, and returns its path.
func writeValidConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("tok\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "a.log"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}
	cfg := "server:\n" +
		"  address: 127.0.0.1:8443\n" +
		"  token_file: " + token + "\n" +
		"state:\n" +
		"  directory: " + filepath.Join(dir, "state") + "\n" +
		"  max_queue_bytes: 1MiB\n" +
		"inputs:\n" +
		"  - id: app\n" +
		"    include:\n" +
		"      - " + filepath.Join(logDir, "*.log") + "\n" +
		"    index: main\n"
	path := filepath.Join(dir, "collector.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func writeBootstrapConfig(t *testing.T) (path, stateDir, tokenPath string) {
	t.Helper()
	dir := t.TempDir()
	stateDir = filepath.Join(dir, "state")
	tokenPath = filepath.Join(dir, "token-that-does-not-exist")
	logPath := filepath.Join(dir, "app.log")
	cfg := "server:\n" +
		"  address: 127.0.0.1:8443\n" +
		"  token_file: " + tokenPath + "\n" +
		"state:\n" +
		"  directory: " + stateDir + "\n" +
		"  max_queue_bytes: 1MiB\n" +
		"inputs:\n" +
		"  - id: app\n" +
		"    include:\n" +
		"      - " + logPath + "\n" +
		"    index: main\n"
	path = filepath.Join(dir, "collector.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write bootstrap config: %v", err)
	}
	return path, stateDir, tokenPath
}

func TestRunIdentityInitializesStableIDWithoutReadingToken(t *testing.T) {
	configPath, stateDir, tokenPath := writeBootstrapConfig(t)
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token fixture unexpectedly exists: %v", err)
	}

	var firstOutput, firstErrors bytes.Buffer
	if got := runIdentity([]string{"-config", configPath}, &firstOutput, &firstErrors); got != 0 {
		t.Fatalf("runIdentity first exit = %d, stderr = %q", got, firstErrors.String())
	}
	firstID := strings.TrimSuffix(firstOutput.String(), "\n")
	if !protocolid.Valid(firstID) || firstOutput.String() != firstID+"\n" {
		t.Fatalf("first identity output = %q, want one canonical ID and newline", firstOutput.String())
	}
	if firstErrors.Len() != 0 {
		t.Fatalf("first identity stderr = %q, want empty", firstErrors.String())
	}

	var secondOutput, secondErrors bytes.Buffer
	if got := runIdentity([]string{"-config", configPath}, &secondOutput, &secondErrors); got != 0 {
		t.Fatalf("runIdentity second exit = %d, stderr = %q", got, secondErrors.String())
	}
	if got := secondOutput.String(); got != firstID+"\n" {
		t.Fatalf("second identity output = %q, want %q", got, firstID+"\n")
	}
	if secondErrors.Len() != 0 {
		t.Fatalf("second identity stderr = %q, want empty", secondErrors.String())
	}
	if data, err := os.ReadFile(filepath.Join(stateDir, "collector_id")); err != nil {
		t.Fatalf("read collector identity: %v", err)
	} else if got := string(data); got != firstID+"\n" {
		t.Fatalf("persisted identity = %q, want %q", got, firstID+"\n")
	}
	for _, forbidden := range []string{"wal", "checkpoints", "dead-letter.jsonl"} {
		if _, err := os.Lstat(filepath.Join(stateDir, forbidden)); !os.IsNotExist(err) {
			t.Fatalf("identity command created forbidden runtime state %q: %v", forbidden, err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(configPath), "app.log")); !os.IsNotExist(err) {
		t.Fatalf("identity command touched its configured input: %v", err)
	}
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("identity command touched its configured token: %v", err)
	}
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "collector_id" && entry.Name() != ".collector.lock" {
			t.Fatalf("identity command created unexpected state entry %q", entry.Name())
		}
	}
}

func TestRunIdentityFailureContracts(t *testing.T) {
	t.Run("missing configuration", func(t *testing.T) {
		assertRunIdentityFailure(
			t,
			[]string{"-config", filepath.Join(t.TempDir(), "missing.yaml")},
			1,
			"load configuration:",
		)
	})
	t.Run("unknown flag", func(t *testing.T) {
		assertRunIdentityFailure(t, []string{"-unknown"}, 2, "flag provided but not defined")
	})
	t.Run("positional argument", func(t *testing.T) {
		assertRunIdentityFailure(t, []string{"unexpected"}, 2, "identity does not accept positional arguments")
	})
	t.Run("corrupt persisted identity", func(t *testing.T) {
		configPath, stateDir, _ := writeBootstrapConfig(t)
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		identityPath := filepath.Join(stateDir, "collector_id")
		if err := os.WriteFile(identityPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		assertRunIdentityFailure(
			t,
			[]string{"-config", configPath},
			1,
			"initialize collector identity:",
		)
		if info, err := os.Stat(identityPath); err != nil || info.Size() != 0 {
			t.Fatalf("corrupt identity was replaced: info=%v err=%v", info, err)
		}
	})
}

func assertRunIdentityFailure(t *testing.T, args []string, wantExit int, wantError string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if got := runIdentity(args, &stdout, &stderr); got != wantExit {
		t.Fatalf("runIdentity(%v) exit = %d, want %d; stderr=%q", args, got, wantExit, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("runIdentity(%v) stdout = %q, want empty", args, stdout.String())
	}
	if !strings.Contains(stderr.String(), wantError) {
		t.Fatalf("runIdentity(%v) stderr = %q, want text %q", args, stderr.String(), wantError)
	}
}

func TestValidateDoesNotInitializeCollectorIdentity(t *testing.T) {
	configPath, stateDir, _ := writeBootstrapConfig(t)
	if got := validateConfig([]string{"-config", configPath}); got != 0 {
		t.Fatalf("validateConfig exit = %d, want 0", got)
	}
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("validate created state directory: %v", err)
	}
}

func TestWriteBuildIdentityReportsCompiledReleaseFields(t *testing.T) {
	t.Parallel()

	identity, err := buildinfo.Current()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := writeBuildIdentity(&output); err != nil {
		t.Fatalf("writeBuildIdentity: %v", err)
	}
	want := "source_revision=" + identity.SourceRevision + "\n"
	if got := output.String(); got != want {
		t.Fatalf("build identity = %q, want %q", got, want)
	}
}

func TestRunDispatch(t *testing.T) {
	t.Parallel()
	valid := writeValidConfig(t)

	tests := []struct {
		name string
		args []string
		want int
	}{
		{"validate ok", []string{"validate", "-config", valid}, 0},
		{"validate missing file", []string{"validate", "-config", "/no/such/file.yaml"}, 1},
		{"validate positional argument", []string{"validate", "unexpected"}, 2},
		{"run positional argument", []string{"run", "unexpected"}, 2},
		{"unknown subcommand", []string{"bogus"}, 2},
		{"help", []string{"help"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := run(tc.args); got != tc.want {
				t.Fatalf("run(%v) = %d, want %d", tc.args, got, tc.want)
			}
		})
	}
}

func TestParseLogLevel(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  slog.Level
	}{
		{value: "debug", want: slog.LevelDebug},
		{value: "INFO", want: slog.LevelInfo},
		{value: "warn", want: slog.LevelWarn},
		{value: "ERROR", want: slog.LevelError},
	} {
		if got, err := parseLogLevel(test.value); err != nil || got != test.want {
			t.Fatalf("parseLogLevel(%q) = %s, %v; want %s", test.value, got, err, test.want)
		}
	}
	if _, err := parseLogLevel("verbose"); err == nil {
		t.Fatal("parseLogLevel accepted an unsupported level")
	}
	if _, err := parseLogLevel("INFO+1"); err == nil {
		t.Fatal("parseLogLevel accepted an undocumented numeric offset")
	}
}

func TestCountMatches(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{"a.log", "b.log", "c.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	got := countMatches([]string{filepath.Join(dir, "*")}, []string{"*.tmp"})
	if got != 2 {
		t.Fatalf("countMatches = %d, want 2 (a.log, b.log)", got)
	}
}
