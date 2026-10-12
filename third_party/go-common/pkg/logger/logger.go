package logger

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger is the global Zap logger instance used throughout the application.
// It should be initialized once during application startup using the Initialize function.
var Logger *zap.Logger

// Initialize builds the global logger from one immutable startup view. Sink and
// output settings remain fixed; the current provider's level is sampled once per entry.
// The returned close function flushes and releases the outputs; call it once
// during shutdown, after the last log entry.
func Initialize(provider config.LoggingProvider) (*zap.Logger, func() error, error) {
	if provider == nil {
		return nil, nil, fmt.Errorf("logger configuration provider cannot be nil")
	}
	startup := provider()
	if startup == nil {
		return nil, nil, fmt.Errorf("logger configuration view cannot be nil")
	}
	levelProvider := func() zapcore.Level { return provider().LogLevel() }

	// Use JSON encoder for structured logging.
	// Although Zap offers a console-friendly development config, using JSON consistently
	// across all environments (including development) provides uniformity, which is
	// beneficial for log aggregation and analysis tools like Grafana.
	encoderConfig := zapcore.EncoderConfig{
		// "timestamp" is the log ENTRY time (when the backend wrote the line). Event
		// timestamps carried in a payload — e.g. Faro/Grafana frontend telemetry — must
		// be logged under a DIFFERENT key (KeyFaroTimestamp = "faro_timestamp") so they
		// never collide with or overwrite this one. See pkg/logger/constants.go.
		TimeKey:        "timestamp",                    // Key for the log-entry timestamp field.
		LevelKey:       "level",                        // Key for the log level field.
		NameKey:        "logger",                       // Key for the logger name field.
		CallerKey:      "caller",                       // Key for the caller (file:line) field.
		MessageKey:     "message",                      // Key for the log message field.
		StacktraceKey:  "stacktrace",                   // Key for the stacktrace field.
		LineEnding:     zapcore.DefaultLineEnding,      // Line ending character(s).
		EncodeLevel:    zapcore.CapitalLevelEncoder,    // Encode log level as capitalized string (e.g., "INFO", "ERROR").
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder, // Encode time with nanosecond precision (e.g., "2025-03-19T09:04:02.422123456-07:00").
		EncodeDuration: zapcore.StringDurationEncoder,  // Encode durations as strings.
		EncodeCaller:   zapcore.ShortCallerEncoder,     // Encode caller as short file:line (e.g., "main.go:123").
	}

	// A custom sink replaces the output paths. The file sink stands alone;
	// network sinks keep a stderr copy. Each call opens its own outputs, so
	// Initialize can run more than once per process.
	sink, err := OpenSink(startup)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to configure log sink: %w", err)
	}
	var out, errOut zapcore.WriteSyncer
	var closeOutputs func() error
	if sink != nil {
		out, closeOutputs = sink, sink.Close
		if startup.LogSink() != config.SinkTypeFile {
			out = zapcore.NewMultiWriteSyncer(zapcore.Lock(os.Stderr), sink)
		}
		errOut = out
	} else {
		outputPaths, errorPaths := startup.LogOutputPaths(), startup.LogErrorOutputPaths()
		if len(outputPaths) == 0 {
			outputPaths = []string{"stderr"}
		}
		if len(errorPaths) == 0 {
			errorPaths = []string{"stderr"}
		}
		var closeOut, closeErrOut func()
		if out, closeOut, err = zap.Open(outputPaths...); err != nil {
			return nil, nil, err
		}
		if errOut, closeErrOut, err = zap.Open(errorPaths...); err != nil {
			closeOut()
			return nil, nil, err
		}
		closeOutputs = func() error { closeOut(); closeErrOut(); return nil }
	}

	// Build the core at Zap's lowest normal level; dynamicLevelCore applies the
	// provider once when checking each entry. No sampler is installed: every
	// admitted event must reach the sink, even repeats sharing a message and severity.
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig), out, zapcore.DebugLevel)
	logger := zap.New(dynamicLevelCore{Core: core, levelProvider: levelProvider},
		zap.ErrorOutput(errOut), zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Set the globally accessible logger instance.
	Logger = logger

	return logger, sync.OnceValue(closeOutputs), nil
}

// dynamicLevelCore keeps Zap's sink/encoding core immutable while consulting a
// concurrency-safe level provider at entry admission time.
type dynamicLevelCore struct {
	zapcore.Core
	levelProvider func() zapcore.Level
}

func (c dynamicLevelCore) Enabled(level zapcore.Level) bool {
	// Enabled is only Zap's inexpensive early-out. Dynamic admission happens in
	// Check so the provider is evaluated exactly once for the complete entry.
	return c.Core.Enabled(level)
}

// Level preserves Zap's level introspection while the provider is hot.
func (c dynamicLevelCore) Level() zapcore.Level {
	return c.levelProvider()
}

func (c dynamicLevelCore) With(fields []zapcore.Field) zapcore.Core {
	return dynamicLevelCore{Core: c.Core.With(fields), levelProvider: c.levelProvider}
}

func (c dynamicLevelCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	minimumLevel := c.levelProvider()
	if entry.Level < minimumLevel {
		return checked
	}
	// Delegate admission to the wrapped core. The dynamic level is intentionally
	// captured at Check time for this entry.
	return c.Core.Check(entry, checked)
}

func WithLayer(logger *zap.Logger, layer string) *zap.Logger {
	return logger.With(zap.String(KeyLayer, layer))
}

func API(logger *zap.Logger) *zap.Logger {
	return WithLayer(logger, "api")
}

func Service(logger *zap.Logger) *zap.Logger {
	return WithLayer(logger, "service")
}

func Persistence(logger *zap.Logger) *zap.Logger {
	return WithLayer(logger, "persistence")
}

// UserIDField renders go-common's user ID type without reflection. Applications
// may pass it as RouterDependencies.UserIDField when constructing SRouter.
func UserIDField(id common_service_models.UserIdType) zap.Field {
	return zap.Uint64(logkeys.UserID, id)
}

// For returns the logger for one operation. When SRouter installed a request
// logger, For reuses its already encoded correlation fields and adds the
// component's relative name. Outside SRouter it preserves the previous
// behavior by deriving the name and context fields from fallback.
//
// name is relative to the application logger. For example, an application
// logger named "myapp" and name "common_service.admin" produce
// "myapp.common_service.admin". Call For once per operation and reuse the
// result. fallback should be the application logger before component naming or
// request correlation is applied.
func For(ctx context.Context, fallback *zap.Logger, name string) *zap.Logger {
	if requestLogger, ok := scontext.GetLogger(ctx); ok {
		return requestLogger.Named(name)
	}
	if fallback == nil {
		return nil
	}
	return WithContextFields(ctx, fallback.Named(name))
}

// DebugEnabled reports whether logger will emit Debug events. SRouter's request
// logger shares the application base logger's core, so gating on the base is
// exact for the derived request logger too. Use it to keep logger derivation
// and field construction inside a diagnostic branch:
//
//	if appLogger.DebugEnabled(h.logger) {
//	    appLogger.For(ctx, h.logger, loggerName).Debug("Cache warmed", fields...)
//	}
//
// When a derived logger already exists, prefer logger.Check(zap.DebugLevel, msg)
// and write through the returned entry instead.
func DebugEnabled(logger *zap.Logger) bool {
	return logger != nil && logger.Core().Enabled(zapcore.DebugLevel)
}

// ContextFields returns every stable per-operation field carried by SRouter
// context, in a fixed order: trace ID, build ID, config ID, and user ID. The
// trace ID is always present, empty when the context carries none, so log
// lines outside a request keep the key; the rest appear only when set. The
// consuming application supplies the build identity; go-common never derives
// one.
//
// The values come from one scontext.GetCorrelation call: a single walk of the
// context chain and a single lock acquisition, rather than one of each per
// field. The user ID is rendered with zap.Uint64 here because UserIdType is a
// uint64 alias; scontext hands it back raw precisely so the caller can pick
// the typed constructor instead of boxing through zap.Any.
func ContextFields(ctx context.Context) []zap.Field {
	c, _ := scontext.GetCorrelation[common_service_models.UserIdType](ctx)
	fields := make([]zap.Field, 0, 4)
	fields = append(fields, zap.String(logkeys.TraceID, c.TraceID))
	if c.HasBuildID() {
		fields = append(fields, zap.String(logkeys.BuildID, c.BuildID))
	}
	if c.HasConfigID() {
		fields = append(fields, zap.String(logkeys.ConfigID, c.ConfigID))
	}
	if c.HasUserID() {
		fields = append(fields, zap.Uint64(KeyUserID, c.UserID))
	}
	return fields
}

// WithContextFields returns logger stamped once with ContextFields(ctx). Call
// it once per request or job at the boundary that owns the context; every
// later log line in that scope inherits the fields for free. Repeating it per
// log line re-encodes the fields each time.
func WithContextFields(ctx context.Context, logger *zap.Logger) *zap.Logger {
	if logger == nil {
		return nil
	}
	return logger.With(ContextFields(ctx)...)
}

// WithTraceID returns logger stamped with the request's trace ID and nothing
// else. Operation boundaries that want every context-carried attribute
// (trace ID, configuration generation, ...) call WithContextFields instead.
func WithTraceID(ctx context.Context, logger *zap.Logger) *zap.Logger {
	if logger == nil {
		return nil
	}
	return logger.With(zap.String(logkeys.TraceID, scontext.GetTraceID(ctx)))
}

// ConfigGenerationFields returns the stable fields that identify the
// configuration generation a sampled view was taken from: KeyConfigRelease,
// KeyConfigReleaseVersion and KeyConfigActivationRevision. It returns no fields
// for views that carry no generation (config.NewRootView, config.Static, custom
// test views), so it is always safe to splat into a log call.
func ConfigGenerationFields(view any) []zap.Field {
	generation, ok := config.GenerationOf(view)
	if !ok {
		return nil
	}
	return generationFields(generation)
}

// WithConfigGeneration returns logger stamped with the generation carried by
// view — the view sampled for the current request or job, so the stamp reflects
// the configuration actually in effect. The logger is returned unchanged when
// it is nil or view carries no generation.
func WithConfigGeneration(logger *zap.Logger, view any) *zap.Logger {
	if logger == nil {
		return nil
	}
	fields := ConfigGenerationFields(view)
	if len(fields) == 0 {
		return logger
	}
	return logger.With(fields...)
}

func generationFields(generation config.Generation) []zap.Field {
	return []zap.Field{
		zap.String(KeyConfigRelease, generation.Release),
		zap.Uint64(KeyConfigReleaseVersion, generation.ReleaseVersion),
		zap.Uint64(KeyConfigActivationRevision, generation.ActivationRevision),
	}
}
