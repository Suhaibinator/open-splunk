package knowledgeattemptaudit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// Appender is the fail-closed dependency intended for future authenticated
// knowledge handlers. A handler must replace its original rejection with a
// generic unavailable response whenever AppendRejected returns an error.
type Appender interface {
	AppendRejected(context.Context, string, Definition) error
}

// Store owns rejected-attempt append and startup integrity validation.
type Store struct {
	orm *gorm.DB
	sql *sql.DB
}

var _ Appender = (*Store)(nil)

type preparedAppend struct {
	actor      audit.Actor
	occurredAt time.Time
}

// New constructs a store over an already migrated control database. It never
// changes schema and validates every retained journal before returning.
func New(database *control.DB) (*Store, error) {
	return NewWithContext(context.Background(), database)
}

// NewWithContext is New with a caller-owned startup-validation context.
func NewWithContext(
	ctx context.Context,
	database *control.DB,
) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf(
			"%w: knowledge-attempt audit startup context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if database == nil || database.GORMDB() == nil || database.SQLDB() == nil {
		return nil, fmt.Errorf(
			"%w: knowledge-attempt audit control database is required",
			control.ErrInvalidArgument,
		)
	}
	if err := validateStartupIntegrity(ctx, database.GORMDB()); err != nil {
		return nil, err
	}
	return &Store{
		orm: database.GORMDB(),
		sql: database.SQLDB(),
	}, nil
}

func validateStartupIntegrity(
	ctx context.Context,
	database *gorm.DB,
) (returnedErr error) {
	tx := database.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return mapStoreError(ctx, "begin startup validation", tx.Error)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil &&
			!errors.Is(rollbackErr, gorm.ErrInvalidTransaction) {
			if returnedErr == nil {
				returnedErr = fmt.Errorf("rollback startup validation: %w", rollbackErr)
			} else {
				returnedErr = errors.Join(returnedErr, rollbackErr)
			}
		}
	}()
	if err := validateAllTenantIntegrity(tx); err != nil {
		return mapStoreError(ctx, "validate startup integrity", err)
	}
	commitErr := tx.Commit().Error
	finished = true
	if commitErr != nil {
		return mapStoreError(ctx, "commit startup validation", commitErr)
	}
	return nil
}

// AppendRejected commits one rejected authenticated attempt in its own
// immediate transaction. This is the normal handler path: it remains durable
// even though the rejected knowledge mutation has already rolled back.
func (store *Store) AppendRejected(
	ctx context.Context,
	tenantID string,
	definition Definition,
) (returnedErr error) {
	prepared, err := prepareAppendInputs(ctx, store, tenantID, definition)
	if err != nil {
		return err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return mapStoreError(ctx, "begin rejected-attempt append", tx.Error)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil &&
			!errors.Is(rollbackErr, gorm.ErrInvalidTransaction) {
			if returnedErr == nil {
				returnedErr = fmt.Errorf("rollback rejected-attempt append: %w", rollbackErr)
			} else {
				returnedErr = errors.Join(returnedErr, rollbackErr)
			}
		}
	}()
	if err := store.appendRejectedPrepared(tx, tenantID, definition, prepared); err != nil {
		return err
	}
	commitErr := tx.Commit().Error
	finished = true
	if commitErr != nil {
		return mapStoreError(ctx, "commit rejected-attempt append", commitErr)
	}
	return nil
}

// AppendRejectedInTransaction appends through a caller-owned transaction. It
// neither commits nor rolls back. Callers must roll back their complete
// transaction on error. Prefer AppendRejected for HTTP rejection journaling.
func (store *Store) AppendRejectedInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	definition Definition,
) error {
	prepared, err := prepareAppendInputs(ctx, store, tenantID, definition)
	if err != nil {
		return err
	}
	if tx == nil || tx.Statement == nil || tx.Config == nil {
		return fmt.Errorf(
			"%w: knowledge-attempt audit transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if _, ok := tx.Statement.ConnPool.(*sql.Tx); !ok {
		return fmt.Errorf(
			"%w: knowledge-attempt audit append requires an active transaction",
			control.ErrInvalidArgument,
		)
	}
	owner, ok := tx.ConnPool.(*sql.DB)
	if !ok || owner != store.sql {
		return fmt.Errorf(
			"%w: knowledge-attempt audit transaction belongs to another database",
			control.ErrInvalidArgument,
		)
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return mapStoreError(ctx, "bind rejected-attempt transaction", database.Error)
	}
	return store.appendRejectedPrepared(database, tenantID, definition, prepared)
}

func (store *Store) appendRejectedPrepared(
	database *gorm.DB,
	tenantID string,
	definition Definition,
	prepared preparedAppend,
) error {
	state, err := ensureTenantState(database, tenantID)
	if err != nil {
		return mapStoreError(database.Statement.Context, "prepare tenant state", err)
	}
	if err := validateTenantAppendBoundary(database, state); err != nil {
		return mapStoreError(database.Statement.Context, "validate retained boundary", err)
	}
	if state.NextSequence > maximumPersistedSequence {
		return fmt.Errorf(
			"%w: knowledge-attempt audit sequence is exhausted",
			control.ErrCapacityExceeded,
		)
	}

	record := eventRecord{
		TenantID:            tenantID,
		Sequence:            state.NextSequence,
		OccurredAtUnixMicro: prepared.occurredAt.UnixMicro(),
		ActorKind:           prepared.actor.Kind,
		ActorID:             prepared.actor.ID,
		ActorRole:           prepared.actor.Role,
		Action:              definition.Action,
		Result:              ResultRejected,
		Reason:              definition.Reason,
	}
	if definition.AuthorizedContext != nil {
		record.AppID = &definition.AuthorizedContext.AppID
		if object := definition.AuthorizedContext.Object; object != nil {
			record.KnowledgeObjectID = &object.KnowledgeObjectID
			objectType := object.ObjectType
			record.ObjectType = &objectType
			version := int64(object.Version) // validated against MaxInt64
			record.ObjectVersion = &version
			scope := object.SharingScope
			record.SharingScope = &scope
		}
	}
	if err := database.Create(&record).Error; err != nil {
		return mapStoreError(database.Statement.Context, "append rejected-attempt event", err)
	}

	advanced, err := readTenantState(database, tenantID)
	if err != nil {
		return mapStoreError(database.Statement.Context, "verify advanced tenant state", err)
	}
	wantFirst := state.FirstSequence
	wantCount := state.RetainedCount + 1
	if state.RetainedCount == MaximumRetainedAttempts {
		wantFirst++
		wantCount--
	}
	if advanced.FirstSequence != wantFirst ||
		advanced.NextSequence != state.NextSequence+1 ||
		advanced.RetainedCount != wantCount {
		return fmt.Errorf(
			"%w: knowledge-attempt audit accounting did not advance exactly once",
			ErrCorrupt,
		)
	}
	return nil
}

func prepareAppendInputs(
	ctx context.Context,
	store *Store,
	tenantID string,
	definition Definition,
) (preparedAppend, error) {
	if ctx == nil {
		return preparedAppend{}, fmt.Errorf("%w: knowledge-attempt audit context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return preparedAppend{}, err
	}
	if store == nil || store.orm == nil || store.sql == nil {
		return preparedAppend{}, fmt.Errorf("%w: knowledge-attempt audit store is unavailable", control.ErrInvalidArgument)
	}
	actor, ok := audit.ActorFromContext(ctx)
	if !ok || !validRejectedActor(actor, definition.Reason) ||
		!validIdentity(tenantID, maximumTenantIDBytes) ||
		!definition.Action.valid() || !definition.Reason.valid() ||
		!validAuthorizedContext(definition.AuthorizedContext, definition.Action, definition.Reason) {
		return preparedAppend{}, fmt.Errorf("%w: knowledge-attempt audit event is invalid", control.ErrInvalidArgument)
	}
	occurredAt, ok := audit.CanonicalOccurrenceTime(definition.OccurredAt)
	if !ok {
		return preparedAppend{}, fmt.Errorf("%w: knowledge-attempt audit timestamp is invalid", control.ErrInvalidArgument)
	}
	return preparedAppend{actor: actor, occurredAt: occurredAt}, nil
}

func ensureTenantState(
	database *gorm.DB,
	tenantID string,
) (tenantStateRecord, error) {
	state, exists, err := readOptionalTenantState(database, tenantID)
	if err != nil {
		return tenantStateRecord{}, err
	}
	if !exists {
		inserted := database.Exec(`
			INSERT INTO knowledge_attempt_audit_tenant_state (
				tenant_id, first_sequence, next_sequence, retained_count
			) VALUES (?, 1, 1, 0)
		`, tenantID)
		if inserted.Error != nil {
			return tenantStateRecord{}, inserted.Error
		}
		if inserted.RowsAffected != 1 {
			return tenantStateRecord{}, fmt.Errorf("%w: tenant state was not created", ErrCorrupt)
		}
		state, err = readTenantState(database, tenantID)
		if err != nil {
			return tenantStateRecord{}, err
		}
	}
	return state, nil
}

func readTenantState(database *gorm.DB, tenantID string) (tenantStateRecord, error) {
	state, exists, err := readOptionalTenantState(database, tenantID)
	if err != nil {
		return tenantStateRecord{}, err
	}
	if !exists {
		return tenantStateRecord{}, fmt.Errorf("%w: tenant state is missing", ErrCorrupt)
	}
	return state, nil
}

func readOptionalTenantState(
	database *gorm.DB,
	tenantID string,
) (tenantStateRecord, bool, error) {
	var records []tenantStateRecord
	query := database.Where("tenant_id = ?", tenantID).Limit(2).Find(&records)
	if query.Error != nil {
		return tenantStateRecord{}, false, query.Error
	}
	if len(records) == 0 {
		return tenantStateRecord{}, false, nil
	}
	if len(records) != 1 || !validTenantState(records[0], tenantID) {
		return tenantStateRecord{}, false, fmt.Errorf("%w: tenant state is invalid", ErrCorrupt)
	}
	return records[0], true, nil
}

func validTenantState(record tenantStateRecord, tenantID string) bool {
	return record.TenantID == tenantID &&
		validIdentity(record.TenantID, maximumTenantIDBytes) &&
		record.FirstSequence >= 1 && record.FirstSequence <= maximumPersistedSequence+1 &&
		record.NextSequence >= record.FirstSequence &&
		record.NextSequence <= maximumPersistedSequence+1 &&
		record.RetainedCount >= 0 && record.RetainedCount <= MaximumRetainedAttempts &&
		record.NextSequence-record.FirstSequence == record.RetainedCount
}

func validateTenantAppendBoundary(database *gorm.DB, state tenantStateRecord) error {
	if !validTenantState(state, state.TenantID) {
		return fmt.Errorf("%w: tenant state is invalid", ErrCorrupt)
	}
	if state.RetainedCount == 0 {
		var eventExists int64
		if err := database.Raw(`
			SELECT EXISTS (
				SELECT 1 FROM knowledge_attempt_audit_events
				WHERE tenant_id = ? LIMIT 1
			)
		`, state.TenantID).Scan(&eventExists).Error; err != nil {
			return err
		}
		if eventExists != 0 {
			return fmt.Errorf("%w: retained boundary does not match tenant state", ErrCorrupt)
		}
		return nil
	}

	expectedBoundaryRows := 2
	if state.FirstSequence == state.NextSequence-1 {
		expectedBoundaryRows = 1
	}
	records, err := readPreflightedRecords(database.
		Model(&eventRecord{}).
		Where(
			"tenant_id = ? AND sequence IN (?, ?)",
			state.TenantID,
			state.FirstSequence,
			state.NextSequence-1,
		).
		Order("sequence ASC"), expectedBoundaryRows)
	if err != nil {
		return err
	}
	if len(records) != expectedBoundaryRows {
		return fmt.Errorf("%w: retained boundary does not match tenant state", ErrCorrupt)
	}
	for index, record := range records {
		expectedSequence := state.FirstSequence
		if index == expectedBoundaryRows-1 {
			expectedSequence = state.NextSequence - 1
		}
		if record.TenantID != state.TenantID || record.Sequence != expectedSequence {
			return fmt.Errorf("%w: retained boundary does not match tenant state", ErrCorrupt)
		}
		if _, err := eventFromRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func mapStoreError(ctx context.Context, operation string, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	return fmt.Errorf("knowledge-attempt audit %s: %w", operation, err)
}
