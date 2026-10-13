package common_service

import (
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// serviceContextualError maps a lower-layer failure to its service-domain
// sentinel while retaining the original cause and diagnostic context for the
// owning boundary record.
func serviceContextualError(domainErr, cause error, fields ...zap.Field) error {
	return appLogger.WithErrorCause(domainErr, cause, fields...)
}
