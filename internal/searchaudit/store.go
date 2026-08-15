package searchaudit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"gorm.io/gorm"
)

// Store owns rolling search-attempt audit append and bounded list traversal.
type Store struct {
	orm                        *gorm.DB
	sql                        *sql.DB
	cursorKey                  []byte
	maximumRetainedAttempts    uint32
	maximumRetainedWasExplicit bool
}

// New constructs a search-attempt audit store over a migrated control-plane
// database. GORM AutoMigrate is deliberately never used.
func New(database *control.DB, options Options) (*Store, error) {
	return NewWithContext(context.Background(), database, options)
}

// NewWithContext constructs a store and bounds its complete persisted-journal
// integrity verification with ctx.
func NewWithContext(
	ctx context.Context,
	database *control.DB,
	options Options,
) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf(
			"%w: search-attempt audit startup context is nil",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if database == nil || database.GORMDB() == nil || database.SQLDB() == nil {
		return nil, fmt.Errorf(
			"%w: search-attempt audit control-plane database is required",
			control.ErrInvalidArgument,
		)
	}
	if len(options.CursorKey) != 0 &&
		(len(options.CursorKey) < minimumCursorKeyBytes ||
			len(options.CursorKey) > maximumCursorKeyBytes) {
		return nil, fmt.Errorf(
			"%w: search-attempt audit cursor key must contain between %d and %d bytes",
			control.ErrInvalidArgument,
			minimumCursorKeyBytes,
			maximumCursorKeyBytes,
		)
	}
	maximum := options.MaximumRetainedAttempts
	explicit := maximum != 0
	if maximum == 0 {
		maximum = DefaultMaximumRetainedAttempts
	}
	if maximum < 1 || maximum > MaximumRetainedAttempts {
		return nil, fmt.Errorf(
			"%w: search-attempt audit retained maximum must be between 1 and %d",
			control.ErrInvalidArgument,
			MaximumRetainedAttempts,
		)
	}
	if err := validateStartupIntegrity(ctx, database.GORMDB()); err != nil {
		return nil, err
	}
	if explicit {
		var mismatch int64
		query := database.GORMDB().WithContext(ctx).Raw(`
			SELECT EXISTS (
				SELECT 1
				FROM search_attempt_audit_tenant_state
				WHERE maximum_retained_attempts <> ?
				LIMIT 1
			)
		`, maximum).Scan(&mismatch)
		if query.Error != nil {
			return nil, mapStoreError(
				ctx,
				"verify configured search-attempt audit retained maximum",
				query.Error,
			)
		}
		if mismatch != 0 {
			return nil, fmt.Errorf(
				"%w: configured search-attempt audit retained maximum does not match persisted tenant state",
				control.ErrInvalidArgument,
			)
		}
	}
	return &Store{
		orm:                        database.GORMDB(),
		sql:                        database.SQLDB(),
		cursorKey:                  slices.Clone(options.CursorKey),
		maximumRetainedAttempts:    maximum,
		maximumRetainedWasExplicit: explicit,
	}, nil
}

// beginReadOnly opens a read-only transaction and returns it with a deferred
// rollback guard. Call markFinished immediately after a successful Commit so
// the guard becomes a no-op; otherwise the guard rolls back and joins any
// rollback failure onto returnedErr.
func beginReadOnly(
	ctx context.Context,
	database *gorm.DB,
	operation string,
	returnedErr *error,
) (tx *gorm.DB, markFinished func(), rollbackGuard func(), err error) {
	tx = database.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return nil, nil, nil, mapStoreError(ctx, "begin "+operation, tx.Error)
	}
	finished := false
	return tx, func() { finished = true }, func() {
		if finished {
			return
		}
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil &&
			!errors.Is(rollbackErr, gorm.ErrInvalidTransaction) {
			rollbackErr = fmt.Errorf("rollback %s: %w", operation, rollbackErr)
			if *returnedErr == nil {
				*returnedErr = rollbackErr
			} else {
				*returnedErr = errors.Join(*returnedErr, rollbackErr)
			}
		}
	}, nil
}

func validateStartupIntegrity(ctx context.Context, database *gorm.DB) (returnedErr error) {
	tx, markFinished, rollbackGuard, err := beginReadOnly(
		ctx,
		database,
		"search-attempt audit startup validation",
		&returnedErr,
	)
	if err != nil {
		return err
	}
	defer rollbackGuard()
	if err := validateAllTenantIntegrity(tx); err != nil {
		return mapStoreError(ctx, "validate search-attempt audit startup integrity", err)
	}
	commitErr := tx.Commit().Error
	markFinished()
	if commitErr != nil {
		return mapStoreError(ctx, "commit search-attempt audit startup validation", commitErr)
	}
	return nil
}

// AppendSearchAttemptInTransaction appends one payload-free attempt through a
// caller-owned transaction. It neither commits nor rolls back tx. Callers must
// roll back their complete search-history admission whenever this returns an
// error.
func (store *Store) AppendSearchAttemptInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	definition searchhistory.SearchAttemptAuditEvent,
) error {
	knowledgeSnapshot, err := validateAppendInputs(ctx, store, tenantID, definition)
	if err != nil {
		return err
	}
	if tx == nil || tx.Statement == nil || tx.Config == nil {
		return fmt.Errorf(
			"%w: search-attempt audit append transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if _, ok := tx.Statement.ConnPool.(*sql.Tx); !ok {
		return fmt.Errorf(
			"%w: search-attempt audit append requires an active SQL transaction",
			control.ErrInvalidArgument,
		)
	}
	owner, ok := tx.ConnPool.(*sql.DB)
	if !ok || owner != store.sql {
		return fmt.Errorf(
			"%w: search-attempt audit transaction belongs to another database",
			control.ErrInvalidArgument,
		)
	}
	database := tx.WithContext(ctx)
	if database.Error != nil {
		return mapStoreError(ctx, "bind search-attempt audit transaction", database.Error)
	}

	state, err := store.ensureTenantState(database, tenantID)
	if err != nil {
		return mapStoreError(ctx, "prepare search-attempt audit tenant state", err)
	}
	if err := validateTenantAppendBoundary(database, state); err != nil {
		return mapStoreError(ctx, "validate search-attempt audit tenant boundary", err)
	}
	if state.NextSequence > maximumPersistedSequence {
		return fmt.Errorf(
			"%w: search-attempt audit tenant sequence is exhausted",
			control.ErrCapacityExceeded,
		)
	}

	occurredAt, _ := audit.CanonicalOccurrenceTime(definition.OccurredAt)
	actor := actorForAppend(ctx)
	record := searchAttemptEventRecord{
		TenantID:            tenantID,
		Sequence:            state.NextSequence,
		OccurredAtUnixMicro: occurredAt.UnixMicro(),
		ActorKind:           actor.Kind,
		ActorID:             actor.ID,
		ActorRole:           actor.Role,
		OwnerID:             definition.OwnerID,
		SearchJobID:         definition.SearchJobID,
	}
	setRecordKnowledgeSnapshot(&record, knowledgeSnapshot)
	if err := database.Create(&record).Error; err != nil {
		return mapStoreError(ctx, "append search-attempt audit event", err)
	}

	// Migration 0023 owns the exact insertion and any rolling prune. Verifying
	// its complete state transition is the bounded postcondition; rehydrating the
	// same immutable boundary rows here would add no independent guarantee.
	advanced, err := readTenantState(database, tenantID)
	if err != nil {
		return mapStoreError(ctx, "verify search-attempt audit tenant state", err)
	}
	wantFirst := state.FirstSequence
	wantCount := state.RetainedCount + 1
	if state.RetainedCount == state.MaximumRetainedAttempts {
		wantFirst++
		wantCount--
	}
	if advanced.FirstSequence != wantFirst ||
		advanced.NextSequence != state.NextSequence+1 ||
		advanced.RetainedCount != wantCount ||
		advanced.MaximumRetainedAttempts != state.MaximumRetainedAttempts {
		return fmt.Errorf(
			"%w: search-attempt audit accounting did not advance exactly once",
			ErrCorrupt,
		)
	}
	return nil
}

func validateAppendInputs(
	ctx context.Context,
	store *Store,
	tenantID string,
	definition searchhistory.SearchAttemptAuditEvent,
) (*opensplunkv1.KnowledgeSnapshotRef, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: search-attempt audit context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || store.orm == nil || store.sql == nil {
		return nil, fmt.Errorf("%w: search-attempt audit store is unavailable", control.ErrInvalidArgument)
	}
	if !validIdentity(tenantID, maximumTenantIDBytes) {
		return nil, fmt.Errorf("%w: search-attempt audit tenant ID is invalid", control.ErrInvalidArgument)
	}
	if _, ok := audit.CanonicalOccurrenceTime(definition.OccurredAt); !ok ||
		!validIdentity(definition.OwnerID, maximumOwnerIDBytes) ||
		!validIdentity(definition.SearchJobID, maximumSearchJobIDBytes) {
		return nil, fmt.Errorf("%w: search-attempt audit event is invalid", control.ErrInvalidArgument)
	}
	return normalizeKnowledgeSnapshotRef(definition.KnowledgeSnapshot)
}

func (store *Store) ensureTenantState(
	database *gorm.DB,
	tenantID string,
) (searchAttemptTenantStateRecord, error) {
	inserted := database.Exec(`
		INSERT INTO search_attempt_audit_tenant_state (
			tenant_id,
			first_sequence,
			next_sequence,
			retained_count,
			maximum_retained_attempts
		)
		SELECT ?, 1, 1, 0, ?
		WHERE NOT EXISTS (
			SELECT 1
			FROM search_attempt_audit_tenant_state
			WHERE tenant_id = ?
		)
	`, tenantID, store.maximumRetainedAttempts, tenantID)
	if inserted.Error != nil {
		return searchAttemptTenantStateRecord{}, inserted.Error
	}
	if inserted.RowsAffected < 0 || inserted.RowsAffected > 1 {
		return searchAttemptTenantStateRecord{}, fmt.Errorf(
			"%w: search-attempt audit tenant initialization affected an invalid row count",
			ErrCorrupt,
		)
	}
	state, err := readTenantState(database, tenantID)
	if err != nil {
		return searchAttemptTenantStateRecord{}, err
	}
	if store.maximumRetainedWasExplicit &&
		state.MaximumRetainedAttempts != int64(store.maximumRetainedAttempts) {
		return searchAttemptTenantStateRecord{}, fmt.Errorf(
			"%w: persisted search-attempt audit retained maximum is %d, configured maximum is %d",
			control.ErrInvalidArgument,
			state.MaximumRetainedAttempts,
			store.maximumRetainedAttempts,
		)
	}
	return state, nil
}

func readTenantState(
	database *gorm.DB,
	tenantID string,
) (searchAttemptTenantStateRecord, error) {
	state, exists, err := readOptionalTenantState(database, tenantID)
	if err != nil {
		return searchAttemptTenantStateRecord{}, err
	}
	if !exists {
		return searchAttemptTenantStateRecord{}, fmt.Errorf(
			"%w: search-attempt audit tenant state is missing",
			ErrCorrupt,
		)
	}
	return state, nil
}

func readOptionalTenantState(
	database *gorm.DB,
	tenantID string,
) (searchAttemptTenantStateRecord, bool, error) {
	var records []searchAttemptTenantStateRecord
	query := database.Where("tenant_id = ?", tenantID).Limit(2).Find(&records)
	if query.Error != nil {
		return searchAttemptTenantStateRecord{}, false, query.Error
	}
	if len(records) == 0 {
		return searchAttemptTenantStateRecord{}, false, nil
	}
	if len(records) != 1 || !validTenantState(records[0], tenantID) {
		return searchAttemptTenantStateRecord{}, false, fmt.Errorf(
			"%w: search-attempt audit tenant state is invalid",
			ErrCorrupt,
		)
	}
	return records[0], true, nil
}

func validTenantState(record searchAttemptTenantStateRecord, tenantID string) bool {
	return record.TenantID == tenantID &&
		validIdentity(record.TenantID, maximumTenantIDBytes) &&
		record.FirstSequence >= 1 &&
		record.FirstSequence <= maximumPersistedSequence+1 &&
		record.NextSequence >= record.FirstSequence &&
		record.NextSequence <= maximumPersistedSequence+1 &&
		record.RetainedCount >= 0 &&
		record.RetainedCount <= MaximumRetainedAttempts &&
		record.MaximumRetainedAttempts >= 1 &&
		record.MaximumRetainedAttempts <= MaximumRetainedAttempts &&
		record.RetainedCount <= record.MaximumRetainedAttempts &&
		record.NextSequence-record.FirstSequence == record.RetainedCount
}

func actorForAppend(ctx context.Context) audit.Actor {
	if actor, ok := audit.ActorFromContext(ctx); ok {
		return actor
	}
	return audit.Actor{
		Kind: audit.ActorKindSystem,
		ID:   defaultSystemActorID,
		Role: audit.ActorRoleSystem,
	}
}

// validateTenantAppendBoundary performs one index-backed scalar precondition.
// Complete record validation happens during construction, while migration 0023
// makes retained rows immutable; the append hot path only needs to prove that
// an empty state has no rows or that both advertised endpoints still exist.
func validateTenantAppendBoundary(
	database *gorm.DB,
	state searchAttemptTenantStateRecord,
) error {
	if !validTenantState(state, state.TenantID) {
		return fmt.Errorf("%w: search-attempt audit tenant state is invalid", ErrCorrupt)
	}
	expectedBoundaryRows := int64(0)
	if state.RetainedCount != 0 {
		expectedBoundaryRows = 2
		if state.FirstSequence == state.NextSequence-1 {
			expectedBoundaryRows = 1
		}
	}
	var boundaryIsValid int64
	if err := database.Raw(`
		SELECT CASE
			WHEN ? = 0 THEN NOT EXISTS (
				SELECT 1
				FROM search_attempt_audit_events
				WHERE tenant_id = ?
				LIMIT 1
			)
			ELSE (
				SELECT COUNT(*)
				FROM search_attempt_audit_events
				WHERE tenant_id = ?
				  AND sequence IN (?, ?)
			) = ?
		END
	`,
		state.RetainedCount,
		state.TenantID,
		state.TenantID,
		state.FirstSequence,
		state.NextSequence-1,
		expectedBoundaryRows,
	).Scan(&boundaryIsValid).Error; err != nil {
		return err
	}
	if boundaryIsValid != 1 {
		return fmt.Errorf(
			"%w: search-attempt audit retained boundary does not match state",
			ErrCorrupt,
		)
	}
	return nil
}

func eventFromRecord(record searchAttemptEventRecord) (Event, error) {
	if record.Sequence < 1 || record.Sequence > maximumPersistedSequence ||
		!validIdentity(record.TenantID, maximumTenantIDBytes) ||
		!validIdentity(record.OwnerID, maximumOwnerIDBytes) ||
		!validIdentity(record.SearchJobID, maximumSearchJobIDBytes) {
		return Event{}, fmt.Errorf("%w: search-attempt audit event scalar is invalid", ErrCorrupt)
	}
	occurredAt, ok := audit.CanonicalOccurrenceTime(time.UnixMicro(record.OccurredAtUnixMicro))
	if !ok || occurredAt.UnixMicro() != record.OccurredAtUnixMicro {
		return Event{}, fmt.Errorf("%w: search-attempt audit event timestamp is invalid", ErrCorrupt)
	}
	actor := audit.Actor{Kind: record.ActorKind, ID: record.ActorID, Role: record.ActorRole}
	if !actor.Valid() {
		return Event{}, fmt.Errorf("%w: search-attempt audit actor is invalid", ErrCorrupt)
	}
	knowledgeSnapshot, err := knowledgeSnapshotFromRecord(record)
	if err != nil {
		return Event{}, err
	}
	event := Event{
		Sequence:          uint64(record.Sequence),
		TenantID:          record.TenantID,
		OccurredAt:        occurredAt,
		Actor:             actor,
		OwnerID:           record.OwnerID,
		SearchJobID:       record.SearchJobID,
		KnowledgeSnapshot: knowledgeSnapshot,
	}.detached()
	if err := event.ValidateForTenant(record.TenantID); err != nil {
		return Event{}, fmt.Errorf("%w: persisted search-attempt audit event is invalid", ErrCorrupt)
	}
	return event, nil
}

func mapStoreError(ctx context.Context, operation string, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
