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
//
// Output is expected to be an unbuffered console sink - stderr, a terminal, a
// pipe, or /dev/null - because Sync suppresses the errno set those sinks
// return for fsync (see Sync). Pointing Output at a buffered writer or a real
// file whose descriptor can be closed underneath the logger would let those
// same errors hide genuinely lost records; such a sink needs its own flush
// contract rather than this one.
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

	// No StacktraceKey and no zap.AddStacktrace: stack capture stays off. The
	// key without the option was dead configuration, and turning the option on
	// would be worse than useless here - SRouter logs every HTTP error,
	// including client 4xx, at Error level, so AddStacktrace(ErrorLevel) would
	// attach a full goroutine stack to every 404. Callers that genuinely want a
	// stack log it as a field on the record they emit.
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "ts",
		NameKey:        "logger",
		CallerKey:      "caller",
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

// Sync flushes buffered logger output. Console sinks - terminals, pipes,
// /dev/null - cannot be flushed with fsync, and rejecting the call is the
// normal answer rather than a lost-output failure, so those errors are
// suppressed. Durability failures on a real file, such as EIO or ENOSPC, are
// not in that set and still propagate.
//
// The suppression is only sound because a process logger writes to an
// unbuffered console sink (see Config.Output): there is no pending buffer to
// lose when the descriptor rejects or has already released its fsync. A
// buffered or regular-file sink must not rely on this function.
func Sync(logger *zap.Logger) error {
	if logger == nil {
		return nil
	}
	return firstDurabilityFailure(logger.Sync())
}

// firstDurabilityFailure walks the whole error tree and returns the first leaf
// that is not an unsyncable-sink error, or nil when every leaf is one.
//
// The tree walk is what makes the suppression safe for a fan-out logger.
// zapcore.NewTee and zapcore.NewMultiWriteSyncer join their per-sink Sync
// errors with go.uber.org/multierr, whose aggregate implements
// Unwrap() []error; errors.Is on that aggregate matches if ANY constituent
// matches, so testing the joined error directly would report success for a
// console sink's EBADF while silently discarding a sibling file sink's EIO or
// ENOSPC. Requiring every leaf to be unsyncable keeps a single real durability
// failure fatal no matter how many sinks are attached.
func firstDurabilityFailure(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		if branches := joined.Unwrap(); len(branches) > 0 {
			for _, branch := range branches {
				if failure := firstDurabilityFailure(branch); failure != nil {
					return failure
				}
			}
			return nil
		}
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if inner := wrapped.Unwrap(); inner != nil {
			if firstDurabilityFailure(inner) == nil {
				return nil
			}
			// Report the outer error so the sink's own context - the path in an
			// fs.PathError, a caller's message - survives.
			return err
		}
	}
	if isUnsyncableSink(err) {
		return nil
	}
	return err
}

// isUnsyncableSink reports whether a single leaf error means the sink does not
// support fsync rather than that buffered output was lost. Operating systems
// disagree on the errno: Linux reports EINVAL for pipes and for character
// devices with no fsync operation, macOS reports EBADF for pipes and ENODEV
// for /dev/null, and other platforms report ENOTTY or ENOTSUP. A sink whose
// descriptor is already closed likewise has nothing left to flush.
func isUnsyncableSink(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.EBADF) ||
		errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, os.ErrClosed)
}
