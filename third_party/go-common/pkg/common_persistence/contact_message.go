package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// CreateContactMessage executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CreateContactMessage(ctx context.Context, message *common_persistence_models.ContactMessage) error {
	if message == nil {
		return appLogger.InvariantError("contact_message_create_requires_model", nil,
			zap.String(logkeys.Operation, "create_contact_message"),
			zap.String(logkeys.Expected, "non-nil contact message"),
			zap.String(logkeys.Actual, "nil contact message"))
	}
	db := h.getDBFromContext(ctx)

	log := appLogger.For(ctx, h.logger, loggerName)
	if ce := log.Check(zap.DebugLevel, "Creating contact message"); ce != nil {
		ce.Write(
			zap.String(logkeys.Operation, "create_contact_message"),
			zap.String(appLogger.KeyEmail, message.Email),
			zap.String(appLogger.KeySubject, message.Subject),
		)
	}

	if err := db.Create(message).Error; err != nil {
		fields := []zap.Field{zap.String(logkeys.Operation, "create_contact_message")}
		fields = append(fields,
			zap.String(appLogger.KeyEmail, message.Email),
			zap.String(appLogger.KeySubject, message.Subject))

		return databaseErrorWithFields(err, fields...)
	}

	log.Info("Contact message created successfully",
		zap.String(logkeys.Operation, "create_contact_message"),
		zap.String(appLogger.KeyOutcome, "created"),
		zap.Uint(appLogger.KeyContactID, message.ID))
	return nil
}
