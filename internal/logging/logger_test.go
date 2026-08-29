package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  zapcore.Level
	}{
		{value: "debug", want: zapcore.DebugLevel},
		{value: "INFO", want: zapcore.InfoLevel},
		{value: " Warn ", want: zapcore.WarnLevel},
		{value: "error", want: zapcore.ErrorLevel},
	} {
		if got, err := ParseLevel(test.value); err != nil || got != test.want {
			t.Errorf("ParseLevel(%q) = (%v, %v), want (%v, nil)", test.value, got, err, test.want)
		}
	}
	for _, value := range []string{"", "verbose", "dpanic", "INFO+1"} {
		if _, err := ParseLevel(value); err == nil {
			t.Errorf("ParseLevel(%q) succeeded", value)
		}
	}
}

func TestParseFormat(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  Format
	}{
		{value: "json", want: FormatJSON},
		{value: "JSON", want: FormatJSON},
		{value: " console ", want: FormatConsole},
	} {
		if got, err := ParseFormat(test.value); err != nil || got != test.want {
			t.Errorf("ParseFormat(%q) = (%q, %v), want (%q, nil)", test.value, got, err, test.want)
		}
	}
	for _, value := range []string{"", "text", "yaml"} {
		if _, err := ParseFormat(value); err == nil {
			t.Errorf("ParseFormat(%q) succeeded", value)
		}
	}
}

func TestNewJSONLoggerIsNamedStructuredAndFiltered(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger, err := New(Config{
		Service: "open-splunk-test",
		Level:   zapcore.InfoLevel,
		Format:  FormatJSON,
		Output:  zapcore.AddSync(&output),
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("hidden")
	logger.Info("ready", zap.Int("inputs", 2))

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("decode JSON log %q: %v", output.String(), err)
	}
	if record["level"] != "info" || record["logger"] != "open-splunk-test" ||
		record["msg"] != "ready" || record["inputs"] != float64(2) {
		t.Fatalf("unexpected JSON record: %#v", record)
	}
	for _, key := range []string{"ts", "caller"} {
		if record[key] == nil || record[key] == "" {
			t.Fatalf("JSON record lacks %s: %#v", key, record)
		}
	}
}

func TestNewConsoleLogger(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger, err := New(Config{
		Service: "open-splunk-test",
		Level:   zapcore.DebugLevel,
		Format:  FormatConsole,
		Output:  zapcore.AddSync(&output),
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("ready", zap.String("mode", "console"))
	for _, want := range []string{"debug", "open-splunk-test", "ready", `"mode": "console"`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("console log %q lacks %q", output.String(), want)
		}
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{Format: FormatJSON}); err == nil {
		t.Fatal("New accepted an empty service")
	}
	if _, err := New(Config{Service: "service", Format: Format("yaml")}); err == nil {
		t.Fatal("New accepted an unsupported format")
	}
}

type syncErrorWriter struct {
	err error
}

func (writer syncErrorWriter) Write(value []byte) (int, error) { return len(value), nil }
func (writer syncErrorWriter) Sync() error                     { return writer.err }

func TestSyncSuppressesOnlyUnsupportedTerminalErrors(t *testing.T) {
	t.Parallel()
	if err := Sync(nil); err != nil {
		t.Fatalf("Sync(nil) = %v", err)
	}
	for _, expected := range []error{syscall.EINVAL, syscall.ENOTTY} {
		logger := zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
			syncErrorWriter{err: expected},
			zapcore.DebugLevel,
		))
		if err := Sync(logger); err != nil {
			t.Fatalf("Sync(%v) = %v", expected, err)
		}
	}
	want := errors.New("flush failed")
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
		syncErrorWriter{err: want},
		zapcore.DebugLevel,
	))
	if err := Sync(logger); !errors.Is(err, want) {
		t.Fatalf("Sync() = %v, want %v", err, want)
	}
}
