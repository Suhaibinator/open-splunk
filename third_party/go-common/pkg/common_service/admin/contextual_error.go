package admin

import (
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func contextualAdminError(domainErr, cause error, fields ...zap.Field) error {
	return appLogger.WithErrorCause(domainErr, cause, fields...)
}

func contextualAdminWarn(domainErr, cause error, fields ...zap.Field) error {
	return appLogger.WithErrorLevel(appLogger.WithErrorCause(domainErr, cause), zapcore.WarnLevel, fields...)
}
