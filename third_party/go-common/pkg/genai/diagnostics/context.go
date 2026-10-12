// Package diagnostics carries an attempt-scoped logger across inference adapters.
package diagnostics

import (
	"context"
	"go.uber.org/zap"
)

type loggerKey struct{}

// WithLogger attaches diagnostics to one attempt without changing a shared provider instance.
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// Logger returns the explicit attempt logger, or fallback when no override is present.
func Logger(ctx context.Context, fallback *zap.Logger) *zap.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*zap.Logger); ok {
		return logger
	}
	return fallback
}
