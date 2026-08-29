// Package logging provides the shared process logger configuration used by the
// Open Splunk server and collector binaries.
package logging

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultLevel  = "info"
	DefaultFormat = "json"
)

// Format selects the encoding used for process log records.
type Format string

const (
	FormatJSON    Format = "json"
	FormatConsole Format = "console"
)

// Config describes one process logger. A nil Output writes to stderr.
type Config struct {
	Service string
	Level   zapcore.Level
	Format  Format
	Output  zapcore.WriteSyncer
}

// ParseLevel accepts the supported operator-facing log levels.
func ParseLevel(value string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf(
			"invalid log level %q: choose debug, info, warn, or error",
			value,
		)
	}
}

// ParseFormat accepts the supported process log encodings.
func ParseFormat(value string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(FormatJSON):
		return FormatJSON, nil
	case string(FormatConsole):
		return FormatConsole, nil
	default:
		return "", fmt.Errorf(
			"invalid log format %q: choose json or console",
			value,
		)
	}
}

// New constructs a named, structured logger with one record per line.
func New(config Config) (*zap.Logger, error) {
	service := strings.TrimSpace(config.Service)
	if service == "" {
		return nil, errors.New("logging service name is required")
	}

	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "ts",
		NameKey:        "logger",
		CallerKey:      "caller",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	var encoder zapcore.Encoder
	switch config.Format {
	case FormatJSON:
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case FormatConsole:
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("unsupported log format %q", config.Format)
	}

	output := config.Output
	if output == nil {
		output = zapcore.AddSync(os.Stderr)
	}
	output = zapcore.Lock(output)
	core := zapcore.NewCore(encoder, output, config.Level)
	return zap.New(
		core,
		zap.AddCaller(),
		zap.ErrorOutput(output),
	).Named(service), nil
}

// Sync flushes buffered logger output. Character devices commonly reject
// fsync; those expected stderr errors do not make an otherwise clean process
// shutdown fail.
func Sync(logger *zap.Logger) error {
	if logger == nil {
		return nil
	}
	err := logger.Sync()
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTTY) {
		return nil
	}
	return err
}
