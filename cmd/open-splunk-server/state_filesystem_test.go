package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func decodeSingleLogLine(t *testing.T, output *bytes.Buffer) map[string]any {
	t.Helper()
	raw := strings.TrimSpace(output.String())
	if raw == "" || strings.Contains(raw, "\n") {
		t.Fatalf("expected exactly one log line, got %q", output.String())
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(raw), &line); err != nil {
		t.Fatalf("decode log line %q: %v", raw, err)
	}
	return line
}

func TestLogControlDatabaseFilesystemReportsRemoteMountsOnceAtErrorLevel(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zapcore.DebugLevel,
	))
	var inspected string
	describe := func(path string) (privatefs.Filesystem, error) {
		inspected = path
		return privatefs.Filesystem{Name: "nfs", Remote: true}, nil
	}
	logControlDatabaseFilesystem(logger, describe, "/var/lib/open-splunk/state/open-splunk.db")

	if inspected != "/var/lib/open-splunk/state" {
		t.Fatalf("inspected %q, want the database directory", inspected)
	}
	line := decodeSingleLogLine(t, &output)
	if line["level"] != "error" {
		t.Fatalf("level = %v, want error: %v", line["level"], line)
	}
	message, _ := line["msg"].(string)
	for _, want := range []string{"network or FUSE filesystem", "SQLite WAL mode is unsafe"} {
		if !strings.Contains(message, want) {
			t.Fatalf("message %q lacks %q", message, want)
		}
	}
	if line["path"] != "/var/lib/open-splunk/state" || line["filesystem"] != "nfs" {
		t.Fatalf("fields = %v", line)
	}
}

func TestLogControlDatabaseFilesystemStaysQuietOnLocalMounts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zapcore.DebugLevel,
	))
	describe := func(string) (privatefs.Filesystem, error) {
		return privatefs.Filesystem{Name: "ext4"}, nil
	}
	logControlDatabaseFilesystem(logger, describe, "/var/lib/open-splunk/state/open-splunk.db")
	if output.Len() != 0 {
		t.Fatalf("local filesystem produced output: %s", output.String())
	}

	// A failed inspection is a warning, never an error and never a crash.
	failing := func(string) (privatefs.Filesystem, error) {
		return privatefs.Filesystem{}, errors.New("statfs: permission denied")
	}
	logControlDatabaseFilesystem(logger, failing, "relative/open-splunk.db")
	line := decodeSingleLogLine(t, &output)
	if line["level"] != "warn" || line["msg"] != "inspect control database filesystem" {
		t.Fatalf("inspection failure line = %v", line)
	}
	wantPath, err := filepath.Abs("relative")
	if err != nil {
		t.Fatal(err)
	}
	if line["path"] != wantPath {
		t.Fatalf("path = %v, want %q", line["path"], wantPath)
	}
}

func TestLogControlDatabaseFilesystemUsesRealProbeOnTemporaryDirectory(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zapcore.DebugLevel,
	))
	logControlDatabaseFilesystem(logger, nil, filepath.Join(t.TempDir(), "open-splunk.db"))
	if output.Len() != 0 {
		t.Fatalf("temporary directory produced output: %s", output.String())
	}
	logControlDatabaseFilesystem(nil, nil, filepath.Join(t.TempDir(), "open-splunk.db"))
}
