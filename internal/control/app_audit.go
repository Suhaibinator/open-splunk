package control

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// AppMutationAuditAction is one fixed successful app mutation. Control owns
// the taxonomy so app persistence can remain independent of the audit package.
type AppMutationAuditAction string

const (
	AppMutationAuditActionCreate   AppMutationAuditAction = "app.create"
	AppMutationAuditActionUpdate   AppMutationAuditAction = "app.update"
	AppMutationAuditActionActivate AppMutationAuditAction = "app.activate"
	AppMutationAuditActionArchive  AppMutationAuditAction = "app.archive"
	AppMutationAuditActionDelete   AppMutationAuditAction = "app.delete"
)

// AppMutationAuditEvent is the complete control-owned projection supplied to
// an audit appender. Tenant identity is supplied separately from trusted scope.
type AppMutationAuditEvent struct {
	OccurredAt time.Time
	Action     AppMutationAuditAction
	AppID      string
	AppVersion uint64
}

// AppMutationAuditAppender publishes one event inside the caller-owned GORM
// transaction. Implementations must not commit or roll back tx.
type AppMutationAuditAppender interface {
	AppendAppMutationInTransaction(
		context.Context,
		*gorm.DB,
		string,
		AppMutationAuditEvent,
	) error
}

// AuditedAppCatalog delegates reads to AppCatalog and makes every successful
// mutation contingent on same-transaction audit publication. The raw catalog
// remains available for internal setup and tests.
type AuditedAppCatalog struct {
	catalog  *AppCatalog
	appender AppMutationAuditAppender
}

// NewAuditedAppCatalog constructs the fail-closed app mutation surface without
// changing the database schema.
func NewAuditedAppCatalog(
	catalog *AppCatalog,
	appender AppMutationAuditAppender,
) (*AuditedAppCatalog, error) {
	if catalog == nil || catalog.orm == nil {
		return nil, fmt.Errorf(
			"%w: app catalog is required",
			ErrInvalidArgument,
		)
	}
	if appender == nil || isNilMutationAuditAppender(appender) {
		return nil, fmt.Errorf(
			"%w: app audit appender is required",
			ErrInvalidArgument,
		)
	}
	return &AuditedAppCatalog{catalog: catalog, appender: appender}, nil
}

func (catalog *AuditedAppCatalog) publish(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event AppMutationAuditEvent,
) error {
	return catalog.appender.AppendAppMutationInTransaction(
		ctx,
		tx,
		tenantID,
		event,
	)
}

// CreateApp creates and audits one active version-one app.
func (catalog *AuditedAppCatalog) CreateApp(
	ctx context.Context,
	scope AppAccessScope,
	definition AppDefinition,
) (AppWorkspace, error) {
	return catalog.catalog.createApp(ctx, scope, definition, catalog.publish)
}

// GetApp delegates one tenant-scoped read without publishing an audit event.
func (catalog *AuditedAppCatalog) GetApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
) (AppWorkspace, error) {
	return catalog.catalog.GetApp(ctx, scope, selector)
}

// ListApps delegates one tenant-scoped read without publishing an audit event.
func (catalog *AuditedAppCatalog) ListApps(
	ctx context.Context,
	scope AppAccessScope,
	request AppListRequest,
) (AppListResult, error) {
	return catalog.catalog.ListApps(ctx, scope, request)
}

// ListAppIdentities delegates one bounded tenant identity read without
// publishing an audit event.
func (catalog *AuditedAppCatalog) ListAppIdentities(
	ctx context.Context,
	scope AppAccessScope,
	maximum uint32,
) (AppIdentityListResult, error) {
	return catalog.catalog.ListAppIdentities(ctx, scope, maximum)
}

// UpdateApp updates and audits one app definition.
func (catalog *AuditedAppCatalog) UpdateApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	definition AppDefinition,
) (AppWorkspace, error) {
	return catalog.catalog.updateApp(
		ctx,
		scope,
		selector,
		expectedVersion,
		definition,
		catalog.publish,
	)
}

// SetAppState applies and audits one reversible lifecycle mutation.
func (catalog *AuditedAppCatalog) SetAppState(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	state AppState,
) (AppWorkspace, error) {
	return catalog.catalog.setAppState(
		ctx,
		scope,
		selector,
		expectedVersion,
		state,
		catalog.publish,
	)
}

// DeleteApp permanently deletes and audits one final archived app version.
func (catalog *AuditedAppCatalog) DeleteApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	confirmationSlug string,
) (string, error) {
	return catalog.catalog.deleteApp(
		ctx,
		scope,
		selector,
		expectedVersion,
		confirmationSlug,
		catalog.publish,
	)
}

type appMutationAuditPublisher func(
	context.Context,
	*gorm.DB,
	string,
	AppMutationAuditEvent,
) error

func publishAppMutationAudit(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	publisher appMutationAuditPublisher,
	event AppMutationAuditEvent,
) error {
	if publisher == nil {
		return nil
	}
	if err := publisher(ctx, tx, tenantID, event); err != nil {
		return fmt.Errorf("append %s audit event: %w", event.Action, err)
	}
	return nil
}
