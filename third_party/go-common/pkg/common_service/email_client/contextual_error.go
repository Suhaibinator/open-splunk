package email_client

import (
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// emailProviderError preserves the provider cause while carrying diagnostics to
// the boundary that owns the single final failure record.
func emailProviderError(domainErr, cause error, fields ...zap.Field) error {
	return appLogger.WithErrorCause(domainErr, cause, fields...)
}
