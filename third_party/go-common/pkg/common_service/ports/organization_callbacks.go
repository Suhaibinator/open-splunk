// Package ports provides interfaces and callback configurations that external
// applications can implement to integrate with go-common lifecycle events.
package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// OrganizationCallbacks defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationCallbacks struct {
	// OnOrganizationCreated is invoked after go-common persists a new organization.
	// Applications can use this hook to create companion records that mirror the core
	// organization state.
	//
	// The callback runs within any active transaction present on the context. Returning
	// an error aborts the operation and rolls back the transaction when possible.
	OnOrganizationCreated func(ctx context.Context, org *common_service_models.Organization) error

	// OnPersonalWorkspaceEnsured is invoked after a personal workspace root and
	// its managed owner membership have been created or repaired. Unlike
	// OnOrganizationCreated, this callback also runs for an already-existing or
	// restored workspace so consumers can idempotently repair projections.
	//
	// The callback runs inside the caller's active transaction. Returning an
	// error aborts the account lifecycle operation.
	OnPersonalWorkspaceEnsured func(ctx context.Context, org *common_service_models.Organization) error

	// OnOrganizationUpdated fires after go-common successfully updates a core organization.
	// The callback receives the latest organization data so applications can synchronize
	// derived projections or search indexes.
	//
	// Errors returned from this callback bubble up to the caller and cause the update to
	// fail, preserving transactional semantics.
	OnOrganizationUpdated func(ctx context.Context, org *common_service_models.Organization) error

	// OnOrganizationDeleted executes before go-common deletes the organization record.
	// Applications can clean up dependent data or enforce referential constraints. If the
	// callback returns an error the delete is aborted.
	OnOrganizationDeleted func(ctx context.Context, orgID common_service_models.OrganizationIdType) error

	// OnOrganizationMemberAdded fires after a user is added to an organization, including
	// direct adds, invite acceptance, and auto-add flows. The membership payload reflects
	// the new membership state.
	//
	// Errors returned from this callback bubble up to the caller and can abort the add
	// operation when running inside a transaction.
	OnOrganizationMemberAdded func(ctx context.Context, membership *common_service_models.UserOrganization) error

	// OnOrganizationMemberRemoved fires after a user is removed from an organization. The
	// membership payload reflects the last known membership state before removal.
	//
	// Errors returned from this callback bubble up to the caller and can abort the removal
	// operation when running inside a transaction.
	OnOrganizationMemberRemoved func(ctx context.Context, membership *common_service_models.UserOrganization) error
}

// OrganizationCallbackConfig defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type OrganizationCallbackConfig[U common_service_models.UserLike] struct {
	Callbacks *OrganizationCallbacks
}

// NewOrganizationCallbackConfig constructs and initializes a new instance in the ports backend flow.
func NewOrganizationCallbackConfig[U common_service_models.UserLike]() *OrganizationCallbackConfig[U] {
	return &OrganizationCallbackConfig[U]{}
}

// WithCallbacks executes its part of the service logic for *OrganizationCallbackConfig[U] in the ports backend flow.
func (c *OrganizationCallbackConfig[U]) WithCallbacks(cb *OrganizationCallbacks) *OrganizationCallbackConfig[U] {
	c.Callbacks = cb
	return c
}
