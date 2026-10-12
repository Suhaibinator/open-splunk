package sms_client

import (
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// smsProviderError preserves the provider cause while carrying diagnostics to
// the boundary that owns the single final failure record.
func smsProviderError(domainErr, cause error, fields ...zap.Field) error {
	return appLogger.WithErrorCause(domainErr, cause, fields...)
}
