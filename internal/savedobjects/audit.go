package savedobjects

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

// SavedSearchMutationAuditAction identifies one successful saved-search
// mutation. The savedobjects package owns this narrow projection so its GORM
// transaction boundary does not depend on the higher-level audit package.
type SavedSearchMutationAuditAction string

const (
	SavedSearchMutationAuditActionCreate    SavedSearchMutationAuditAction = "saved_search.create"
	SavedSearchMutationAuditActionUpdate    SavedSearchMutationAuditAction = "saved_search.update"
	SavedSearchMutationAuditActionDuplicate SavedSearchMutationAuditAction = "saved_search.duplicate"
	SavedSearchMutationAuditActionDelete    SavedSearchMutationAuditAction = "saved_search.delete"
)

// SavedSearchMutationAuditEvent is the complete definition-free projection
// supplied to the audit journal inside the live mutation transaction.
type SavedSearchMutationAuditEvent struct {
	OccurredAt         time.Time
	Action             SavedSearchMutationAuditAction
	SavedSearchID      string
	SavedSearchVersion uint64
}

// SavedSearchMutationAuditAppender publishes one event through the
// caller-owned GORM transaction without committing or rolling it back.
type SavedSearchMutationAuditAppender interface {
	AppendSavedSearchMutationInTransaction(
		context.Context,
		*gorm.DB,
		string,
		SavedSearchMutationAuditEvent,
	) error
}

// AuditedStore delegates reads to Store and makes every successful mutation
// contingent on same-transaction audit publication. Raw Store remains useful
// for isolated setup and persistence tests; production constructs this type.
type AuditedStore struct {
	store    *Store
	tenantID string
	appender SavedSearchMutationAuditAppender
}

// NewAuditedStore binds a raw saved-search store to one trusted tenant and one
// same-database audit appender.
func NewAuditedStore(
	store *Store,
	tenantID string,
	appender SavedSearchMutationAuditAppender,
) (*AuditedStore, error) {
	if store == nil || store.orm == nil {
		return nil, fmt.Errorf(
			"%w: saved-search store is required",
			control.ErrInvalidArgument,
		)
	}
	if strings.TrimSpace(tenantID) != tenantID ||
		validateIdentifierText("saved-search audit tenant ID", tenantID, 255, false) != nil {
		return nil, fmt.Errorf(
			"%w: saved-search audit tenant ID is invalid",
			control.ErrInvalidArgument,
		)
	}
	if nilcheck.IsNil(appender) {
		return nil, fmt.Errorf(
			"%w: saved-search audit appender is required",
			control.ErrInvalidArgument,
		)
	}
	return &AuditedStore{
		store:    store,
		tenantID: strings.Clone(tenantID),
		appender: appender,
	}, nil
}

func (store *AuditedStore) publish(
	ctx context.Context,
	tx *gorm.DB,
	event SavedSearchMutationAuditEvent,
) error {
	return store.appender.AppendSavedSearchMutationInTransaction(
		ctx,
		tx,
		store.tenantID,
		event,
	)
}

// Create persists and audits one normalized version-one definition.
func (store *AuditedStore) Create(
	ctx context.Context,
	scope AccessScope,
	definition *opensplunkv1.SavedSearchDefinition,
) (*opensplunkv1.SavedSearch, error) {
	return store.store.create(ctx, scope, definition, store.publish)
}

// Get delegates an owner-scoped read without publishing an audit event.
func (store *AuditedStore) Get(
	ctx context.Context,
	scope AccessScope,
	id string,
) (*opensplunkv1.SavedSearch, error) {
	return store.store.Get(ctx, scope, id)
}

// List delegates a bounded owner-scoped traversal without publishing an audit
// event.
func (store *AuditedStore) List(
	ctx context.Context,
	scope AccessScope,
	request ListRequest,
) (ListResult, error) {
	return store.store.List(ctx, scope, request)
}

// Update applies and audits one optimistic definition mutation.
func (store *AuditedStore) Update(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
	definition *opensplunkv1.SavedSearchDefinition,
	updateMask *fieldmaskpb.FieldMask,
) (*opensplunkv1.SavedSearch, error) {
	return store.store.update(
		ctx,
		scope,
		id,
		expectedVersion,
		definition,
		updateMask,
		store.publish,
	)
}

// Duplicate clones and audits a new version-one saved search. The event
// targets the new object; it deliberately carries no source definition or ID.
func (store *AuditedStore) Duplicate(
	ctx context.Context,
	scope AccessScope,
	sourceID string,
	newName string,
	destinationAppID *string,
) (*opensplunkv1.SavedSearch, error) {
	return store.store.duplicate(
		ctx,
		scope,
		sourceID,
		newName,
		destinationAppID,
		store.publish,
	)
}

// Delete removes and audits the last retained version of one owned saved
// search.
func (store *AuditedStore) Delete(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
) error {
	return store.store.delete(ctx, scope, id, expectedVersion, store.publish)
}

type savedSearchMutationAuditPublisher func(
	context.Context,
	*gorm.DB,
	SavedSearchMutationAuditEvent,
) error

func publishSavedSearchMutationAudit(
	ctx context.Context,
	tx *gorm.DB,
	publisher savedSearchMutationAuditPublisher,
	event SavedSearchMutationAuditEvent,
) error {
	if publisher == nil {
		return nil
	}
	if err := publisher(ctx, tx, event); err != nil {
		return fmt.Errorf("append %s audit event: %w", event.Action, err)
	}
	return nil
}
