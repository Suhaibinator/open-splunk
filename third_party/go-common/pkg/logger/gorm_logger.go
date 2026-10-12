package logger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// GormLogger adapts a Zap logger to GORM's logging interface.
type GormLogger struct {
	// ZapLogger is the underlying Zap logger instance used for logging.
	ZapLogger *zap.Logger
	// LogLevel specifies the minimum level at which GORM logs will be captured.
	LogLevel gormlogger.LogLevel
	// SlowThreshold defines the duration after which a query is considered slow and logged as a warning.
	SlowThreshold time.Duration
	// IgnoreRecordNotFoundError determines whether gorm.ErrRecordNotFound errors should be ignored.
	IgnoreRecordNotFoundError bool
}

func NewGormLogger(zapLogger *zap.Logger) *GormLogger {
	return &GormLogger{
		ZapLogger:                 zapLogger.With(zap.String(KeyComponent, "gorm")),
		LogLevel:                  gormlogger.Info,
		SlowThreshold:             200 * time.Millisecond,
		IgnoreRecordNotFoundError: true,
	}
}

func (l *GormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

func (l *GormLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Info {
		l.ZapLogger.Info("GORM informational event",
			zap.String(KeyGORMMessage, fmt.Sprintf(msg, data...)))
	}
}

func (l *GormLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Warn {
		l.ZapLogger.Warn("GORM warning event",
			zap.String(KeyGORMMessage, fmt.Sprintf(msg, data...)))
	}
}

func (l *GormLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Error {
		l.ZapLogger.Error("GORM error event",
			zap.String(KeyGORMMessage, fmt.Sprintf(msg, data...)))
	}
}

func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	fields := []zapcore.Field{
		zap.String(KeySQL, sql),
		zap.Int64(KeyRows, rows),
		DurationMS(KeyElapsedMS, elapsed),
	}

	// Log based on error and execution time
	switch {
	case err != nil && l.LogLevel >= gormlogger.Error && (!errors.Is(err, gorm.ErrRecordNotFound) || !l.IgnoreRecordNotFoundError):
		l.ZapLogger.Error("GORM error", append(fields, zap.Error(err))...)
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= gormlogger.Warn:
		l.ZapLogger.Warn("GORM slow query", fields...)
	case l.LogLevel >= gormlogger.Info:
		if ce := l.ZapLogger.Check(zapcore.DebugLevel, "GORM query"); ce != nil {
			ce.Write(fields...)
		}
	}
}

func (l *GormLogger) SetSlowThreshold(threshold time.Duration) {
	l.SlowThreshold = threshold
}

func (l *GormLogger) SetIgnoreRecordNotFoundError(ignore bool) {
	l.IgnoreRecordNotFoundError = ignore
}
