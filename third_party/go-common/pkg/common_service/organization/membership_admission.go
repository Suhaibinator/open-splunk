package organization

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

type membershipAdmissionDiagnostics struct {
	operation        string
	actorID          common_service_models.UserIdType
	orgID            common_service_models.OrganizationIdType
	targetUserID     *common_service_models.UserIdType
	actorErrorDomain error
}

func (d membershipAdmissionDiagnostics) fields(extra ...zap.Field) []zap.Field {
	fields := []zap.Field{
		zap.String(logkeys.Operation, d.operation),
		zap.Uint64(appLogger.KeyActorUserID, uint64(d.actorID)),
	}
	if d.targetUserID != nil {
		fields = append(fields, zap.Uint64(appLogger.KeyUserID, uint64(*d.targetUserID)))
	}
	fields = append(fields, zap.Int64(appLogger.KeyOrganizationID, d.orgID))
	return append(fields, extra...)
}

func (d membershipAdmissionDiagnostics) actorError(err error) error {
	fields := d.fields(zap.String(logkeys.Stage, "resolve_actor_membership"))
	if d.actorErrorDomain != nil {
		return appLogger.WithErrorCause(d.actorErrorDomain, err, fields...)
	}
	return appLogger.WithErrorFields(err, fields...)
}

func (d membershipAdmissionDiagnostics) withTarget(userID common_service_models.UserIdType) membershipAdmissionDiagnostics {
	d.targetUserID = &userID
	return d
}

func (d membershipAdmissionDiagnostics) errorAtStage(err error, stage string) error {
	return appLogger.WithErrorFields(err, d.fields(zap.String(logkeys.Stage, stage))...)
}

func (d membershipAdmissionDiagnostics) internalErrorAtStage(cause error, stage string) error {
	return appLogger.WithErrorCause(errors.ErrInternalError, cause, d.fields(zap.String(logkeys.Stage, stage))...)
}

func (d membershipAdmissionDiagnostics) denied() error {
	return appLogger.WithErrorFields(errors.ErrInsufficientPermissions, d.fields(
		zap.String(logkeys.Reason, "insufficient_permission"),
	)...)
}

func (d membershipAdmissionDiagnostics) bulkCapabilityError(err error) error {
	return appLogger.WithErrorFields(err, d.fields(
		zap.String(logkeys.Reason, "bulk_member_management_capability_denied"),
	)...)
}

// resolveMembershipAdmissionActorAndValidateBulkCapability resolves the actor's
// membership against the original request organization ID before applying the
// site-admin bulk-member capability gate to the resolved organization.
func (h *OrganizationHandler[U]) resolveMembershipAdmissionActorAndValidateBulkCapability(
	ctx context.Context,
	actor common_service_models.UserLike,
	requestOrgID common_service_models.OrganizationIdType,
	resolvedOrg *resolvedOrganization,
	diagnostics membershipAdmissionDiagnostics,
) (proto_models.UserType, proto_models.OrganizationRole, error) {
	actorType, actorRole, _, err := h.getActorOrgMembership(ctx, actor, requestOrgID)
	if err != nil {
		return actorType, actorRole, diagnostics.actorError(err)
	}
	if h.permissionHandler != nil && h.permissionHandler.HasSitePermission(ctx, actorType, permission.PermissionAdminManageOrganizationMembers) {
		if err := h.ensureOrganizationTypeCapabilityForOrganization(resolvedOrg.Organization, organizationTypeCapabilityBulkMemberManagement); err != nil {
			return actorType, actorRole, diagnostics.bulkCapabilityError(err)
		}
	}
	return actorType, actorRole, nil
}
