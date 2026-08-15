package control

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"gorm.io/gorm"
)

// IndexMutationAuditAction is one fixed successful index mutation. The
// control package owns this narrow taxonomy so its transaction boundary does
// not need to import a higher-level audit implementation.
type IndexMutationAuditAction string

const (
	IndexMutationAuditActionCreate         IndexMutationAuditAction = "index.create"
	IndexMutationAuditActionUpdate         IndexMutationAuditAction = "index.update"
	IndexMutationAuditActionActivate       IndexMutationAuditAction = "index.activate"
	IndexMutationAuditActionArchive        IndexMutationAuditAction = "index.archive"
	IndexMutationAuditActionDeleteKeepData IndexMutationAuditAction = "index.delete_keep_data"
	IndexMutationAuditActionDeleteData     IndexMutationAuditAction = "index.delete_data"
)

// IndexMutationAuditEvent is the complete control-owned projection supplied
// to an audit appender. It deliberately carries no index definition, request
// body, free-form metadata, or caller-selected tenant.
type IndexMutationAuditEvent struct {
	OccurredAt   time.Time
	Action       IndexMutationAuditAction
	IndexID      string
	IndexVersion uint64
}

// IndexMutationAuditAppender publishes one event inside the caller-owned GORM
// transaction. Implementations must not commit or roll back tx. They are also
// responsible for validating any trusted actor carried by ctx.
type IndexMutationAuditAppender interface {
	AppendIndexMutationInTransaction(
		context.Context,
		*gorm.DB,
		string,
		IndexMutationAuditEvent,
	) error
}

// AuditedIndexAdministrationOptions binds the production mutation surface to
// one trusted deployment tenant, one same-database transaction appender, and
// an optional higher-level index-name validator. A nil Validator is safe only
// when both sparse ACTIVE-knowledge drivers prove there is no ACTIVE object.
type AuditedIndexAdministrationOptions struct {
	TenantID  string
	Appender  IndexMutationAuditAppender
	Validator IndexNameAdmissionValidator
}

// AuditedIndexAdministration delegates index reads to DB and makes every
// successful mutation contingent on publishing its audit event in the same
// SQLite transaction. Raw DB methods remain available for internal setup and
// tests that do not represent production administrative mutations.
type AuditedIndexAdministration struct {
	db        *DB
	tenantID  string
	appender  IndexMutationAuditAppender
	validator IndexNameAdmissionValidator
}

// NewAuditedIndexAdministration constructs the fail-closed production index
// mutation surface without changing the database schema.
func NewAuditedIndexAdministration(
	db *DB,
	options AuditedIndexAdministrationOptions,
) (*AuditedIndexAdministration, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf(
			"%w: index control-plane database is required",
			ErrInvalidArgument,
		)
	}
	if err := validateTenantID(options.TenantID); err != nil {
		return nil, fmt.Errorf(
			"%w: index audit tenant ID is invalid",
			ErrInvalidArgument,
		)
	}
	if nilcheck.IsNil(options.Appender) {
		return nil, fmt.Errorf(
			"%w: index audit appender is required",
			ErrInvalidArgument,
		)
	}
	if options.Validator != nil && nilcheck.IsNil(options.Validator) {
		return nil, fmt.Errorf(
			"%w: index-name admission validator is nil",
			ErrInvalidArgument,
		)
	}
	return &AuditedIndexAdministration{
		db:        db,
		tenantID:  strings.Clone(options.TenantID),
		appender:  options.Appender,
		validator: options.Validator,
	}, nil
}

func (administration *AuditedIndexAdministration) publish(
	ctx context.Context,
	tx *gorm.DB,
	event IndexMutationAuditEvent,
) error {
	return administration.appender.AppendIndexMutationInTransaction(
		ctx,
		tx,
		administration.tenantID,
		event,
	)
}

// CreateIndex creates and audits one index at version one.
func (administration *AuditedIndexAdministration) CreateIndex(
	ctx context.Context,
	definition IndexDefinition,
) (Index, error) {
	return administration.db.createIndex(
		ctx,
		definition,
		administration.publish,
		administration.validator,
	)
}

// GetIndex delegates one stable-ID lookup to the underlying catalog.
func (administration *AuditedIndexAdministration) GetIndex(
	ctx context.Context,
	id string,
) (Index, error) {
	return administration.db.GetIndex(ctx, id)
}

// GetIndexByName delegates one canonical-name lookup to the underlying
// catalog.
func (administration *AuditedIndexAdministration) GetIndexByName(
	ctx context.Context,
	name string,
) (Index, error) {
	return administration.db.GetIndexByName(ctx, name)
}

// ListIndexPage delegates one bounded metadata page to the underlying
// catalog.
func (administration *AuditedIndexAdministration) ListIndexPage(
	ctx context.Context,
	request IndexListRequest,
) (IndexListResult, error) {
	return administration.db.ListIndexPage(ctx, request)
}

// UpdateIndex updates and audits one mutable index definition.
func (administration *AuditedIndexAdministration) UpdateIndex(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	definition IndexDefinition,
) (Index, error) {
	return administration.db.updateIndex(
		ctx,
		id,
		expectedVersion,
		definition,
		administration.publish,
	)
}

// SetIndexState applies and audits one reversible lifecycle mutation.
func (administration *AuditedIndexAdministration) SetIndexState(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	state IndexState,
) (Index, error) {
	return administration.db.setIndexState(
		ctx,
		id,
		expectedVersion,
		state,
		administration.publish,
	)
}

// DeleteIndex atomically writes the KEEP_DATA tombstone and its audit event.
func (administration *AuditedIndexAdministration) DeleteIndex(
	ctx context.Context,
	id string,
	expectedVersion uint64,
	confirmationName string,
) (string, error) {
	return administration.db.deleteIndex(
		ctx,
		id,
		expectedVersion,
		confirmationName,
		administration.publish,
	)
}

// BeginIndexDataDeletion atomically admits and audits a fresh DELETE_DATA
// operation. An exact retry returns the existing operation without publishing
// another event. The supplied scope must match the constructor-bound tenant.
func (administration *AuditedIndexAdministration) BeginIndexDataDeletion(
	ctx context.Context,
	scope IndexDataDeletionScope,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
) (IndexDeletionOperation, error) {
	if scope.TenantID != administration.tenantID {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: index data deletion tenant does not match audit scope",
			ErrInvalidArgument,
		)
	}
	return administration.db.beginIndexDataDeletionWithAudit(
		ctx,
		scope,
		indexID,
		expectedVersion,
		confirmationName,
		administration.publish,
	)
}

type indexMutationAuditPublisher func(
	context.Context,
	*gorm.DB,
	IndexMutationAuditEvent,
) error

func publishIndexMutationAudit(
	ctx context.Context,
	tx *gorm.DB,
	publisher indexMutationAuditPublisher,
	event IndexMutationAuditEvent,
) error {
	if publisher == nil {
		return nil
	}
	if err := publisher(ctx, tx, event); err != nil {
		return fmt.Errorf("append %s audit event: %w", event.Action, err)
	}
	return nil
}
