package organization

import (
	"context"
	stdErrors "errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// GetOrganizationDeletionImpactPage returns every kind of descendant, including
// organizations outside the caller's memberships. It therefore requires the
// site-wide organization-list permission, just like the admin organization table.
// The root is excluded; successive pages use the last returned organization ID.
// The caller bounds limit (the HTTP sanitizer clamps it to the pagination maximum).
func (h *OrganizationHandler[U]) GetOrganizationDeletionImpactPage(ctx context.Context, organizationID, afterOrganizationID common_service_models.OrganizationIdType, limit int) ([]*common_service_models.Organization, bool, error) {
	if organizationID <= 0 || afterOrganizationID < 0 {
		return nil, false, errors.ErrInvalidOrganizationID
	}
	if limit <= 0 {
		return nil, false, errors.ErrInvalidRequest
	}
	if _, err := h.requireAdminOrganizationListPermission(ctx); err != nil {
		return nil, false, err
	}
	if _, err := h.persistenceHandler.GetOrganizationByID(ctx, organizationID); err != nil {
		if stdErrors.Is(err, persistence.ErrOrganizationNotFound) {
			return nil, false, errors.ErrOrganizationNotFound
		}
		return nil, false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_organization_deletion_impact"),
			zap.String(logkeys.Stage, "load_root"), zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	rows, hasMore, err := h.persistenceHandler.GetOrganizationDescendantsPage(ctx, organizationID, afterOrganizationID, limit)
	if err != nil {
		return nil, false, appLogger.WithErrorCause(errors.ErrInternalError, err,
			zap.String(logkeys.Operation, "get_organization_deletion_impact"),
			zap.String(logkeys.Stage, "load_descendants"), zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	organizations := make([]*common_service_models.Organization, 0, len(rows))
	for _, row := range rows {
		organizations = append(organizations, h.converter.PersistenceOrganizationToService(row))
	}
	return organizations, hasMore, nil
}
