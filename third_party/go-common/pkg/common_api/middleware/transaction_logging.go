package middleware

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

const commonAPILoggerName = "common_api"

func transactionRequestLogFields[U common_service_models.UserLike](r *http.Request, handlerErr error, handlerErrorSet bool) []zap.Field {
	fields := []zap.Field{
		zap.Bool(appLogger.KeyHandlerErrorSet, handlerErrorSet),
	}
	if r != nil {
		fields = append(fields,
			zap.String(logkeys.Method, r.Method),
		)
		if r.URL != nil {
			fields = append(fields, zap.String(logkeys.Path, r.URL.Path))
		}
		if user, found := scontext.GetUser[common_service_models.UserIdType, U](r.Context()); found && user != nil {
			fields = append(fields, zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())))
		}
	}
	if handlerErr != nil {
		fields = append(fields, zap.NamedError(appLogger.KeyHandlerError, handlerErr))
	}
	return fields
}
