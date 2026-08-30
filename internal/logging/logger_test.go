package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
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

// TestNewOmitsStacktraces pins the decision not to capture stacks. SRouter
// logs every HTTP error, client 4xx included, at Error level, so enabling
// zap.AddStacktrace(ErrorLevel) would attach a goroutine stack to every 404.
func TestNewOmitsStacktraces(t *testing.T) {
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
	logger.Error("failed", zap.Error(errors.New("boom")))

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("decode JSON log %q: %v", output.String(), err)
	}
	if _, present := record["stacktrace"]; present {
		t.Fatalf("error record carries a stacktrace: %#v", record)
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

// TestSyncToleratesConsoleSinks covers the real descriptors a process logger is
// pointed at in production. fsync on a pipe reports EBADF on macOS and EINVAL
// on Linux, and /dev/null reports ENODEV on macOS; none of those mean buffered
// output was lost, so Sync must report success. Release CI pipes stderr, so a
// regression here fails `make release` after the work already succeeded.
//
// Every case here closes the *os.File it opened. A previous "descriptor closed
// underneath the sink" case called syscall.Close on writer.Fd() and left the
// *os.File unclosed on purpose; os.Pipe installs a runtime finalizer on that
// file, so once it was collected the runtime closed the same fd NUMBER after
// the kernel had already handed it to an unrelated open in another parallel
// subtest, which then failed with "bad file descriptor". The EBADF branch of
// isUnsyncableSink is covered deterministically by
// TestSyncSuppressesOnlyUnsupportedTerminalErrors instead, and the "closed
// file" case below still exercises a real descriptor.
func TestSyncToleratesConsoleSinks(t *testing.T) {
	t.Parallel()

	newFileLogger := func(t *testing.T, file *os.File) *zap.Logger {
		t.Helper()
		logger, err := New(Config{
			Service: "open-splunk-test",
			Level:   zapcore.InfoLevel,
			Format:  FormatJSON,
			Output:  zapcore.AddSync(file),
		})
		if err != nil {
			t.Fatal(err)
		}
		return logger
	}

	t.Run("pipe write end", func(t *testing.T) {
		t.Parallel()
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		defer writer.Close()
		// Drain the pipe so a logged record cannot block on the 64 KiB buffer.
		go func() { _, _ = io.Copy(io.Discard, reader) }()

		logger := newFileLogger(t, writer)
		logger.Info("written to a pipe")
		if err := Sync(logger); err != nil {
			t.Fatalf("Sync(pipe) = %v, want nil", err)
		}
	})

	t.Run("dev null", func(t *testing.T) {
		t.Parallel()
		devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer devNull.Close()

		logger := newFileLogger(t, devNull)
		logger.Info("written to /dev/null")
		if err := Sync(logger); err != nil {
			t.Fatalf("Sync(%s) = %v, want nil", os.DevNull, err)
		}
	})

	t.Run("closed file", func(t *testing.T) {
		t.Parallel()
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		logger := newFileLogger(t, writer)
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := Sync(logger); err != nil {
			t.Fatalf("Sync(closed file) = %v, want nil", err)
		}
	})
}

// TestSyncPropagatesRealFlushFailures keeps the suppression narrow: a sink that
// loses output must still fail.
func TestSyncPropagatesRealFlushFailures(t *testing.T) {
	t.Parallel()
	for _, expected := range []error{syscall.EIO, syscall.ENOSPC, errors.New("flush failed")} {
		logger := zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
			syncErrorWriter{err: expected},
			zapcore.DebugLevel,
		))
		if err := Sync(logger); !errors.Is(err, expected) {
			t.Fatalf("Sync(%v) = %v, want %v", expected, err, expected)
		}
	}
}

// TestSyncPropagatesFailuresFromFannedOutSinks pins the tree walk in
// firstDurabilityFailure. zapcore.NewMultiWriteSyncer and zapcore.NewTee join
// their per-sink errors with go.uber.org/multierr, whose aggregate implements
// Unwrap() []error, so errors.Is on the aggregate matches when ANY constituent
// matches. Testing the joined error directly would have reported success for a
// console sink's EBADF while throwing away a file sink's EIO.
func TestSyncPropagatesFailuresFromFannedOutSinks(t *testing.T) {
	t.Parallel()

	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"})

	mixed := zap.New(zapcore.NewCore(
		encoder,
		zapcore.NewMultiWriteSyncer(
			syncErrorWriter{err: syscall.EBADF},
			syncErrorWriter{err: syscall.EIO},
		),
		zapcore.DebugLevel,
	))
	if err := Sync(mixed); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Sync(multi[EBADF, EIO]) = %v, want an error wrapping %v", err, syscall.EIO)
	}

	wrappedFailure := fmt.Errorf("sync audit log: %w", syscall.ENOSPC)
	teeMixed := zap.New(zapcore.NewTee(
		zapcore.NewCore(encoder, syncErrorWriter{err: syscall.EBADF}, zapcore.DebugLevel),
		zapcore.NewCore(encoder, syncErrorWriter{err: wrappedFailure}, zapcore.DebugLevel),
	))
	if err := Sync(teeMixed); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Sync(tee[EBADF, ENOSPC]) = %v, want an error wrapping %v", err, syscall.ENOSPC)
	}

	allUnsyncable := zap.New(zapcore.NewTee(
		zapcore.NewCore(encoder, syncErrorWriter{err: syscall.EBADF}, zapcore.DebugLevel),
		zapcore.NewCore(
			encoder,
			syncErrorWriter{err: &fs.PathError{Op: "sync", Path: "/dev/stderr", Err: syscall.EBADF}},
			zapcore.DebugLevel,
		),
	))
	if err := Sync(allUnsyncable); err != nil {
		t.Fatalf("Sync(tee[EBADF, EBADF]) = %v, want nil", err)
	}
}

func TestSyncSuppressesOnlyUnsupportedTerminalErrors(t *testing.T) {
	t.Parallel()
	if err := Sync(nil); err != nil {
		t.Fatalf("Sync(nil) = %v", err)
	}
	for _, expected := range []error{
		syscall.EINVAL,
		syscall.ENOTTY,
		syscall.EBADF,
		syscall.ENODEV,
		syscall.ENOTSUP,
		os.ErrClosed,
	} {
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
