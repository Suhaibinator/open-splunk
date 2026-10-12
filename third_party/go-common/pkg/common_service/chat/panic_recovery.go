package chat

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// runWithPanicRecovery is the final safety net for detached/background chat work.
// Durable tool-execution panics are recovered earlier at the executor boundary; this
// wrapper prevents an unexpected panic in surrounding orchestration from terminating
// the process and lets scanners continue with later ticks/items.
func (h *Handler[U]) runWithPanicRecovery(ctx context.Context, operation string, fn func(), fields ...zap.Field) {
	defer h.recoverPanic(ctx, operation, fields...)
	fn()
}

// recoverPanic is intended to be called directly with defer at a goroutine or
// work-item boundary.
func (h *Handler[U]) recoverPanic(ctx context.Context, operation string, fields ...zap.Field) {
	if recovered := recover(); recovered != nil {
		h.logRecoveredPanic(ctx, operation, recovered, fields...)
	}
}

func (h *Handler[U]) logRecoveredPanic(ctx context.Context, operation string, recovered any, fields ...zap.Field) {
	logger := h.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	fields = append(fields,
		zap.String(logkeys.Operation, operation),
		zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)),
		zap.ByteString(appLogger.KeyStacktrace, debug.Stack()),
	)
	appLogger.For(ctx, logger, chatLoggerName).Error("background chat work panicked; recovered", fields...)
}
