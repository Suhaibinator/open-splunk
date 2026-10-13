package logger

import (
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func OperationFields(operation string, err error, fields ...zap.Field) []zap.Field {
	contextFields := ErrorFields(err)
	logFields := make([]zap.Field, 0, len(fields)+len(contextFields)+2)
	if operation != "" {
		logFields = append(logFields, zap.String(logkeys.Operation, operation))
	}
	for _, field := range fields {
		if err != nil && field.Key == logkeys.Error {
			continue
		}
		logFields = append(logFields, field)
	}
	for _, field := range contextFields {
		if err != nil && field.Key == logkeys.Error {
			continue
		}
		logFields = append(logFields, field)
	}
	if err != nil {
		logFields = append(logFields, zap.Error(err))
	}
	return deduplicateFields(logFields)
}

func LogOperationError(
	logger *zap.Logger,
	message string,
	operation string,
	err error,
	fields ...zap.Field,
) {
	LogOperation(logger, zapcore.ErrorLevel, message, operation, err, fields...)
}

func LogOperationWarn(
	logger *zap.Logger,
	message string,
	operation string,
	err error,
	fields ...zap.Field,
) {
	LogOperation(logger, zapcore.WarnLevel, message, operation, err, fields...)
}

// LogOperation emits through an already-derived logger (see For), honoring
// severity and diagnostic fields attached to err.
func LogOperation(
	logger *zap.Logger,
	level zapcore.Level,
	message string,
	operation string,
	err error,
	fields ...zap.Field,
) {
	if logger == nil {
		return
	}
	if override, ok := ErrorLevel(err); ok {
		level = override
	}

	if level < zapcore.DebugLevel || level > zapcore.WarnLevel {
		// Operation events are Debug, Info, Warn, or Error; anything else,
		// including Panic and Fatal, is emitted as Error and never terminates.
		level = zapcore.ErrorLevel
	}
	// Resolve the level before building fields so a disabled entry costs one
	// core check and no allocations.
	if ce := logger.Check(level, message); ce != nil {
		ce.Write(OperationFields(operation, err, fields...)...)
	}
}
