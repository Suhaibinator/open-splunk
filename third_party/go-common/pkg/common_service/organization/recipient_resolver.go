package organization

import (
	"context"
	stdErrors "errors"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

type recipientResolutionDiagnostics struct {
	operation           string
	emailLookupStage    string
	usernameLookupStage string
	extraFields         []zap.Field
}

var organizationIdentifierResolutionDiagnostics = recipientResolutionDiagnostics{
	operation:           "resolve_organization_user_identifier",
	emailLookupStage:    "lookup_by_email",
	usernameLookupStage: "lookup_by_username",
}

func (d recipientResolutionDiagnostics) fields(stage string) []zap.Field {
	fields := []zap.Field{zap.String(logkeys.Operation, d.operation)}
	if stage != "" {
		fields = append(fields, zap.String(logkeys.Stage, stage))
	}
	return append(fields, d.extraFields...)
}

func (h *OrganizationHandler[U]) resolveOrganizationUserByIdentifier(
	ctx context.Context,
	identifierType proto_models.UserIdentifierType,
	identifier string,
	diagnostics recipientResolutionDiagnostics,
) (*common_persistence_models.User, string, error) {
	switch identifierType {
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_EMAIL:
		email := strings.ToLower(identifier)
		user, err := h.persistenceHandler.GetUserByEmail(ctx, email)
		if err != nil && !stdErrors.Is(err, persistence.ErrUserNotFound) {
			fields := append(diagnostics.fields(diagnostics.emailLookupStage), zap.String(appLogger.KeyEmail, email))
			return nil, "", appLogger.WithErrorCause(errors.ErrInternalError, err, fields...)
		}
		return user, email, nil
	case proto_models.UserIdentifierType_USER_IDENTIFIER_TYPE_USERNAME:
		user, err := h.persistenceHandler.GetUserByUsername(ctx, identifier)
		if err != nil {
			if stdErrors.Is(err, persistence.ErrUserNotFound) {
				fields := append(diagnostics.fields(""),
					zap.String(appLogger.KeyIdentifier, identifier),
					zap.String(logkeys.Reason, "username_not_found"),
				)
				return nil, "", appLogger.WithErrorFields(errors.ErrUserNotFound, fields...)
			}
			fields := append(diagnostics.fields(diagnostics.usernameLookupStage), zap.String(appLogger.KeyIdentifier, identifier))
			return nil, "", appLogger.WithErrorCause(errors.ErrInternalError, err, fields...)
		}
		return user, user.Email, nil
	default:
		return nil, "", errors.ErrInvalidRequest
	}
}
