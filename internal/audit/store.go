package audit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// StoreOptions configures audit persistence. An empty CursorKey constructs an
// append-only store; List then fails closed without touching storage. A
// nonempty key must carry at least 256 bits and is detached from the caller.
type StoreOptions struct {
	CursorKey []byte
}

// Store owns immutable event append and bounded list traversal.
type Store struct {
	orm       *gorm.DB
	sql       *sql.DB
	cursorKey []byte
}

// TransactionAppender is the narrow dependency needed by another
// control-plane store to include audit publication in its own GORM transaction.
// A caller must roll back that transaction whenever AppendInTransaction returns
// an error.
type TransactionAppender interface {
	AppendInTransaction(
		context.Context,
		*gorm.DB,
		string,
		SuccessfulEvent,
	) (Event, error)
}

var _ TransactionAppender = (*Store)(nil)

// NewStore constructs an audit store over the migrated control database. GORM
// AutoMigrate is deliberately never invoked.
func NewStore(database *control.DB, options StoreOptions) (*Store, error) {
	return NewStoreWithContext(context.Background(), database, options)
}

// NewStoreWithContext constructs an audit store and bounds its complete
// persisted-journal startup verification with ctx. GORM AutoMigrate is
// deliberately never invoked.
func NewStoreWithContext(
	ctx context.Context,
	database *control.DB,
	options StoreOptions,
) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: audit startup context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf(
			"%w: audit control-plane database is required",
			control.ErrInvalidArgument,
		)
	}
	if len(options.CursorKey) != 0 &&
		(len(options.CursorKey) < minimumCursorKeyBytes ||
			len(options.CursorKey) > maximumCursorKeyBytes) {
		return nil, fmt.Errorf(
			"%w: audit cursor key must contain between %d and %d bytes",
			control.ErrInvalidArgument,
			minimumCursorKeyBytes,
			maximumCursorKeyBytes,
		)
	}
	if err := validateStartupIntegrity(ctx, database.GORMDB()); err != nil {
		return nil, err
	}
	return &Store{
		orm:       database.GORMDB(),
		sql:       database.SQLDB(),
		cursorKey: slices.Clone(options.CursorKey),
	}, nil
}

func validateStartupIntegrity(
	ctx context.Context,
	database *gorm.DB,
) (returnedErr error) {
	tx := database.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return mapStoreError(ctx, "begin audit startup validation", tx.Error)
	}
	transactionFinished := false
	defer finishAuditTransaction(tx, "startup validation", &transactionFinished, &returnedErr)

	if err := validateAllTenantIntegrity(tx); err != nil {
		return mapStoreError(ctx, "validate audit startup integrity", err)
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return mapStoreError(ctx, "commit audit startup validation", commitErr)
	}
	return nil
}

// Append commits one event in a package-owned transaction. Control-plane
// mutation stores should instead call AppendInTransaction before committing
// their own mutation transaction.
func (store *Store) Append(
	ctx context.Context,
	tenantID string,
	definition SuccessfulEvent,
) (event Event, returnedErr error) {
	if err := validateAppendInputs(ctx, store, tenantID, definition); err != nil {
		return Event{}, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Event{}, mapStoreError(ctx, "begin audit append", tx.Error)
	}
	transactionFinished := false
	defer finishAuditTransaction(tx, "append", &transactionFinished, &returnedErr)

	event, err := store.AppendInTransaction(ctx, tx, tenantID, definition)
	if err != nil {
		return Event{}, err
	}
	commitErr := tx.Commit().Error
	transactionFinished = true
	if commitErr != nil {
		return Event{}, mapStoreError(ctx, "commit audit append", commitErr)
	}
	return event, nil
}

// AppendInTransaction inserts one successful event through caller-owned tx.
// It neither commits nor rolls back tx. The caller must treat a returned event
// as provisional until its complete control-plane transaction commits.
func (store *Store) AppendInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	definition SuccessfulEvent,
) (Event, error) {
	if err := validateAppendInputs(ctx, store, tenantID, definition); err != nil {
		return Event{}, err
	}
	if tx == nil || tx.Statement == nil || tx.Config == nil {
		return Event{}, fmt.Errorf(
			"%w: audit append transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if _, ok := tx.Statement.ConnPool.(*sql.Tx); !ok {
		return Event{}, fmt.Errorf(
			"%w: audit append requires an active SQL transaction",
			control.ErrInvalidArgument,
		)
	}
	owner, ok := tx.ConnPool.(*sql.DB)
	if !ok || owner != store.sql {
		return Event{}, fmt.Errorf(
			"%w: audit append transaction belongs to another database",
			control.ErrInvalidArgument,
		)
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return Event{}, mapStoreError(ctx, "bind audit append transaction", database.Error)
	}

	state, err := ensureTenantState(database, tenantID)
	if err != nil {
		return Event{}, mapStoreError(ctx, "prepare audit tenant state", err)
	}
	if _, err := readValidatedTenantTail(database, state); err != nil {
		return Event{}, mapStoreError(ctx, "validate audit tenant boundary", err)
	}
	if state.EventCount == MaximumEventsPerTenant {
		return Event{}, fmt.Errorf(
			"%w: audit tenant has reached its %d-event limit",
			control.ErrCapacityExceeded,
			MaximumEventsPerTenant,
		)
	}

	occurredAt, ok := CanonicalOccurrenceTime(definition.OccurredAt)
	if !ok {
		return Event{}, fmt.Errorf(
			"%w: audit event timestamp is unsupported",
			control.ErrInvalidArgument,
		)
	}
	actor := actorForAppend(ctx)
	if !validSuccessfulActorForAction(actor, definition.Action) {
		return Event{}, fmt.Errorf(
			"%w: audit actor cannot perform successful mutations",
			control.ErrInvalidArgument,
		)
	}
	record := auditEventRecord{
		TenantID:            tenantID,
		Sequence:            state.NextSequence,
		OccurredAtUnixMicro: occurredAt.UnixMicro(),
		ActorKind:           actor.Kind,
		ActorID:             actor.ID,
		ActorRole:           actor.Role,
		Action:              definition.Action,
		TargetKind:          definition.TargetKind,
		TargetID:            definition.TargetID,
		// TargetVersion was validated against math.MaxInt64.
		TargetVersion: int64(definition.TargetVersion), // #nosec G115
	}
	if definition.TargetKind == TargetKindKnowledgeObject {
		appID := strings.Clone(definition.KnowledgeObject.AppID)
		objectType := definition.KnowledgeObject.ObjectType
		sharingScope := definition.KnowledgeObject.SharingScope
		record.AppID = &appID
		record.ObjectType = &objectType
		record.SharingScope = &sharingScope
	}
	if err := database.Create(&record).Error; err != nil {
		return Event{}, mapStoreError(ctx, "append audit event", err)
	}

	advanced, err := readTenantState(database, tenantID)
	if err != nil {
		return Event{}, mapStoreError(ctx, "verify advanced audit tenant state", err)
	}
	if advanced.EventCount != state.EventCount+1 ||
		advanced.NextSequence != state.NextSequence+1 {
		return Event{}, fmt.Errorf(
			"%w: audit tenant accounting did not advance exactly once",
			ErrCorrupt,
		)
	}
	event, err := eventFromRecord(record)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func validateAppendInputs(
	ctx context.Context,
	store *Store,
	tenantID string,
	definition SuccessfulEvent,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: audit context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.orm == nil || store.sql == nil {
		return fmt.Errorf("%w: audit store is unavailable", control.ErrInvalidArgument)
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if !definition.valid() {
		return fmt.Errorf("%w: successful audit event is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func ensureTenantState(database *gorm.DB, tenantID string) (auditTenantStateRecord, error) {
	inserted := database.Exec(`
		INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count)
		SELECT ?, 1, 0
		WHERE NOT EXISTS (
			SELECT 1
			FROM audit_tenant_state
			WHERE tenant_id = ?
		)
	`, tenantID, tenantID)
	if inserted.Error != nil {
		return auditTenantStateRecord{}, inserted.Error
	}
	if inserted.RowsAffected < 0 || inserted.RowsAffected > 1 {
		return auditTenantStateRecord{}, fmt.Errorf(
			"%w: audit tenant initialization affected an invalid row count",
			ErrCorrupt,
		)
	}
	return readTenantState(database, tenantID)
}

func readTenantState(database *gorm.DB, tenantID string) (auditTenantStateRecord, error) {
	state, exists, err := readOptionalTenantState(database, tenantID)
	if err != nil {
		return auditTenantStateRecord{}, err
	}
	if !exists {
		return auditTenantStateRecord{}, fmt.Errorf("%w: audit tenant state is missing", ErrCorrupt)
	}
	return state, nil
}

func validTenantState(record auditTenantStateRecord, tenantID string) bool {
	return record.TenantID == tenantID &&
		validIdentity(record.TenantID, maximumTenantIDBytes) &&
		record.EventCount >= 0 &&
		record.EventCount <= MaximumEventsPerTenant &&
		record.NextSequence >= 1 &&
		record.NextSequence <= MaximumEventsPerTenant+1 &&
		record.NextSequence == record.EventCount+1
}

func readValidatedTenantTail(
	database *gorm.DB,
	state auditTenantStateRecord,
) (*auditEventRecord, error) {
	records, err := readPreflightedAuditRecords(database.
		Model(&auditEventRecord{}).
		Where("tenant_id = ?", state.TenantID).
		Order("sequence DESC").
		Limit(2), 2)
	if err != nil {
		return nil, err
	}
	if state.EventCount == 0 {
		if len(records) != 0 {
			return nil, fmt.Errorf("%w: empty audit tenant contains events", ErrCorrupt)
		}
		return nil, nil
	}
	if len(records) == 0 ||
		records[0].Sequence != state.EventCount ||
		records[0].TenantID != state.TenantID {
		return nil, fmt.Errorf("%w: audit tenant tail does not match state", ErrCorrupt)
	}
	if _, err := eventFromRecord(records[0]); err != nil {
		return nil, err
	}
	return &records[0], nil
}

func eventFromRecord(record auditEventRecord) (Event, error) {
	if record.Sequence < 1 || record.Sequence > MaximumEventsPerTenant ||
		!validIdentity(record.TenantID, maximumTenantIDBytes) ||
		!validIdentity(record.TargetID, maximumTargetIDBytes) ||
		record.TargetVersion < 1 {
		return Event{}, fmt.Errorf("%w: audit event scalar is invalid", ErrCorrupt)
	}
	occurredAt, ok := CanonicalOccurrenceTime(time.UnixMicro(record.OccurredAtUnixMicro))
	if !ok || occurredAt.UnixMicro() != record.OccurredAtUnixMicro {
		return Event{}, fmt.Errorf("%w: audit event timestamp is invalid", ErrCorrupt)
	}
	actor := Actor{
		Kind: record.ActorKind,
		ID:   record.ActorID,
		Role: record.ActorRole,
	}
	definition := SuccessfulEvent{
		OccurredAt:    occurredAt,
		Action:        record.Action,
		TargetKind:    record.TargetKind,
		TargetID:      record.TargetID,
		TargetVersion: uint64(record.TargetVersion),
	}
	metadataPresent := record.AppID != nil
	if (record.ObjectType != nil) != metadataPresent ||
		(record.SharingScope != nil) != metadataPresent {
		return Event{}, fmt.Errorf("%w: audit knowledge metadata is incomplete", ErrCorrupt)
	}
	if metadataPresent {
		definition.KnowledgeObject = KnowledgeObjectMetadata{
			AppID:        *record.AppID,
			ObjectType:   *record.ObjectType,
			SharingScope: *record.SharingScope,
		}
	}
	if !validSuccessfulActorForAction(actor, definition.Action) ||
		!definition.valid() {
		return Event{}, fmt.Errorf("%w: audit event taxonomy is invalid", ErrCorrupt)
	}
	return Event{
		Sequence:        uint64(record.Sequence),
		TenantID:        record.TenantID,
		OccurredAt:      occurredAt,
		Actor:           actor,
		Action:          definition.Action,
		TargetKind:      definition.TargetKind,
		TargetID:        definition.TargetID,
		TargetVersion:   definition.TargetVersion,
		KnowledgeObject: definition.KnowledgeObject,
	}.detached(), nil
}

func mapStoreError(ctx context.Context, operation string, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// finishAuditTransaction rolls back tx unless it was already finished, joining
// any rollback failure onto the error the enclosing function is returning.
func finishAuditTransaction(tx *gorm.DB, label string, finished *bool, returnedErr *error) {
	if tx == nil || finished == nil || *finished {
		return
	}
	rollbackErr := tx.Rollback().Error
	if rollbackErr == nil || errors.Is(rollbackErr, gorm.ErrInvalidTransaction) {
		return
	}
	wrapped := fmt.Errorf("rollback audit %s: %w", label, rollbackErr)
	if returnedErr == nil {
		return
	}
	if *returnedErr == nil {
		*returnedErr = wrapped
		return
	}
	*returnedErr = errors.Join(*returnedErr, wrapped)
}
