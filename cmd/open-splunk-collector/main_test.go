package main

import (
	"os"
	"path/filepath"
	"testing"
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
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "a.log"), []byte("{}\n"), 0o644); err != nil {
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
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
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
