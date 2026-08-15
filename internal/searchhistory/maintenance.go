package searchhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

// MaintenanceCursor is opaque continuation state for count retention. It
// either resumes one over-capacity owner or continues the ordered scope scan
// after a completed owner. Callers must return it unchanged on the next batch.
type MaintenanceCursor struct {
	tenantID  string
	ownerID   string
	scanAfter bool
}

// MaintenancePruneResult describes one committed, bounded maintenance step.
type MaintenancePruneResult struct {
	Deleted int64
	More    bool
	Cursor  *MaintenanceCursor
}

// PruneMaintenanceBatch physically applies the configured terminal age and
// per-owner count bounds across every persisted tenant/owner scope. At most
// maximumRows are deleted before it returns. Pending attempts are deliberately
// excluded because they must be recovered into terminal audit history.
//
// Cursor is opaque continuation state for the ordered count-retention scan.
// Passing the returned cursor avoids revisiting completed owner scopes and
// repeating discovery work for a large over-capacity scope.
func (store *Store) PruneMaintenanceBatch(
	ctx context.Context,
	maximumRows int,
	cursor *MaintenanceCursor,
) (MaintenancePruneResult, error) {
	if err := validateContext(ctx); err != nil {
		return MaintenancePruneResult{}, err
	}
	if maximumRows <= 0 || maximumRows > maximumMaintenancePruneBatchSize {
		return MaintenancePruneResult{}, invalid(fmt.Sprintf(
			"maintenance prune maximum rows must be between 1 and %d",
			maximumMaintenancePruneBatchSize,
		))
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return MaintenancePruneResult{}, errors.New(
			"maintain search history: clock returned an invalid timestamp",
		)
	}

	result := MaintenancePruneResult{Cursor: cloneMaintenanceCursor(cursor)}
	remaining := int64(maximumRows)
	ageBudget := maximumRows
	if result.Cursor != nil {
		ageBudget = maximumRows / 2
		if ageBudget == 0 {
			ageBudget = 1
		}
	}
	expired, ageBatchFull, err := store.pruneExpiredTransaction(
		ctx,
		now,
		ageBudget,
	)
	result.Deleted = expired
	if err != nil {
		return result, err
	}
	remaining -= expired
	if remaining == 0 {
		result.More = ageBatchFull || result.Cursor != nil
		return result, nil
	}

	for remaining > 0 {
		if result.Cursor == nil || result.Cursor.scanAfter {
			next, found, err := store.discoverOverflowCursor(
				ctx,
				result.Cursor,
			)
			if err != nil {
				return result, err
			}
			if !found {
				result.Cursor = nil
				result.More = ageBatchFull
				return result, nil
			}
			result.Cursor = next
		}

		batchDeleted, stillOverflow, err := store.pruneOverflowTransaction(
			ctx,
			result.Cursor,
			remaining,
		)
		if err != nil {
			return result, err
		}
		result.Deleted += batchDeleted
		remaining -= batchDeleted
		if stillOverflow {
			result.More = true
			return result, nil
		}
		result.Cursor = &MaintenanceCursor{
			tenantID:  result.Cursor.tenantID,
			ownerID:   result.Cursor.ownerID,
			scanAfter: true,
		}
	}

	result.More = ageBatchFull || result.Cursor != nil
	return result, nil
}

func cloneMaintenanceCursor(cursor *MaintenanceCursor) *MaintenanceCursor {
	if cursor == nil {
		return nil
	}
	return &MaintenanceCursor{
		tenantID:  cursor.tenantID,
		ownerID:   cursor.ownerID,
		scanAfter: cursor.scanAfter,
	}
}

func (store *Store) pruneExpiredTransaction(
	ctx context.Context,
	now time.Time,
	maximumRows int,
) (deleted int64, more bool, returnedErr error) {
	cutoff := now.Add(-store.maximumAge).UnixMicro()
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, false, mapContextError(
			ctx,
			"begin expired search-history prune batch",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &returnedErr)
	expired := tx.Model(&historyRecord{}).
		Select("search_job_id").
		Where("created_at_unix_micro < ?", cutoff).
		Order("created_at_unix_micro").
		Order("search_job_id").
		Limit(maximumRows)
	deleteResult := tx.
		Where("search_job_id IN (?)", expired).
		Delete(&historyRecord{})
	if deleteResult.Error != nil {
		return 0, false, mapContextError(
			ctx,
			"prune expired search-history batch",
			deleteResult.Error,
		)
	}
	if deleteResult.RowsAffected < 0 ||
		deleteResult.RowsAffected > int64(maximumRows) {
		return 0, false, errors.New(
			"prune expired search-history batch: invalid affected row count",
		)
	}
	if err := tx.Commit().Error; err != nil {
		return 0, false, mapContextError(
			ctx,
			"commit expired search-history prune batch",
			err,
		)
	}
	deleted = deleteResult.RowsAffected
	return deleted, deleted == int64(maximumRows), nil
}

func (store *Store) discoverOverflowCursor(
	ctx context.Context,
	after *MaintenanceCursor,
) (*MaintenanceCursor, bool, error) {
	var record historyOwnerCountRecord
	query := store.orm.WithContext(ctx).
		Model(&historyOwnerCountRecord{}).
		Select("tenant_id", "owner_id", "terminal_count").
		Where("terminal_count > ?", store.maximumEntriesPerOwner)
	if after != nil {
		scope, err := normalizeScope(AccessScope{
			TenantID: after.tenantID,
			OwnerID:  after.ownerID,
		})
		if err != nil {
			return nil, false, invalid(
				"search-history maintenance cursor is invalid",
			)
		}
		query = query.Where(
			"(tenant_id > ? OR (tenant_id = ? AND owner_id > ?))",
			scope.TenantID,
			scope.TenantID,
			scope.OwnerID,
		)
	}
	query = query.
		Order("tenant_id").
		Order("owner_id").
		Limit(1).
		Take(&record)
	switch {
	case query.Error == nil:
		scope, err := persistedMaintenanceScope(record)
		if err != nil {
			return nil, false, err
		}
		return &MaintenanceCursor{
			tenantID: scope.TenantID,
			ownerID:  scope.OwnerID,
		}, true, nil
	case errors.Is(query.Error, gorm.ErrRecordNotFound):
		return nil, false, nil
	default:
		return nil, false, mapContextError(
			ctx,
			"discover over-capacity search-history scope",
			query.Error,
		)
	}
}

func (store *Store) pruneOverflowTransaction(
	ctx context.Context,
	cursor *MaintenanceCursor,
	maximumRows int64,
) (deleted int64, more bool, returnedErr error) {
	if cursor == nil {
		return 0, false, errors.New(
			"prune over-capacity search history: cursor is required",
		)
	}
	scope, err := normalizeScope(AccessScope{
		TenantID: cursor.tenantID,
		OwnerID:  cursor.ownerID,
	})
	if err != nil {
		return 0, false, invalid("search-history maintenance cursor is invalid")
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, false, mapContextError(
			ctx,
			"begin over-capacity search-history prune batch",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &returnedErr)

	deleted, more, err = store.deleteOldestExcess(
		tx,
		scope,
		maximumRows,
	)
	if err != nil {
		return 0, false, mapContextError(
			ctx,
			"prune over-capacity search-history batch",
			err,
		)
	}
	if deleted == 0 {
		if err := tx.Commit().Error; err != nil {
			return 0, false, mapContextError(
				ctx,
				"commit empty over-capacity search-history prune batch",
				err,
			)
		}
		return 0, false, nil
	}
	if err := tx.Commit().Error; err != nil {
		return 0, false, mapContextError(
			ctx,
			"commit over-capacity search-history prune batch",
			err,
		)
	}
	return deleted, more, nil
}

func persistedMaintenanceScope(record historyOwnerCountRecord) (AccessScope, error) {
	scope, err := normalizeScope(AccessScope{
		TenantID: record.TenantID,
		OwnerID:  record.OwnerID,
	})
	if err != nil {
		return AccessScope{}, persistedDataError(
			"persisted search-history maintenance scope is invalid",
			err,
		)
	}
	if record.TerminalCount <= 0 {
		return AccessScope{}, errors.New(
			"persisted search-history owner count is not positive",
		)
	}
	return scope, nil
}

func (store *Store) deleteOldestExcess(
	database *gorm.DB,
	scope AccessScope,
	maximumRows int64,
) (int64, bool, error) {
	if maximumRows <= 0 {
		return 0, false, errors.New(
			"prune search history by count: batch size must be positive",
		)
	}
	var record historyOwnerCountRecord
	query := database.
		Where("tenant_id = ? AND owner_id = ?", scope.TenantID, scope.OwnerID).
		Take(&record)
	switch {
	case errors.Is(query.Error, gorm.ErrRecordNotFound):
		return 0, false, nil
	case query.Error != nil:
		return 0, false, fmt.Errorf(
			"read search-history owner count: %w",
			query.Error,
		)
	}
	persistedScope, err := persistedMaintenanceScope(record)
	if err != nil {
		return 0, false, err
	}
	if persistedScope != scope {
		return 0, false, errors.New(
			"search-history owner count query returned a cross-scope row",
		)
	}
	excess := record.TerminalCount -
		int64(store.maximumEntriesPerOwner)
	if excess <= 0 {
		return 0, false, nil
	}
	deleteLimit := min(excess, maximumRows)
	oldest := database.Model(&historyRecord{}).
		Select("search_job_id").
		Where("tenant_id = ? AND owner_id = ?", scope.TenantID, scope.OwnerID).
		Order("created_at_unix_micro").
		Order("search_job_id").
		Limit(int(deleteLimit))
	deleteResult := database.
		Where(
			"tenant_id = ? AND owner_id = ? AND search_job_id IN (?)",
			scope.TenantID,
			scope.OwnerID,
			oldest,
		).
		Delete(&historyRecord{})
	if deleteResult.Error != nil {
		return 0, false, fmt.Errorf(
			"delete excess search-history rows: %w",
			deleteResult.Error,
		)
	}
	if deleteResult.RowsAffected <= 0 ||
		deleteResult.RowsAffected > deleteLimit {
		return 0, false, errors.New(
			"delete excess search-history rows: invalid affected row count",
		)
	}
	return deleteResult.RowsAffected,
		deleteResult.RowsAffected < excess,
		nil
}

func (store *Store) pruneScopeTransaction(
	ctx context.Context,
	scope AccessScope,
	now time.Time,
	maximumRows int,
) (deleted int64, more bool, returnedErr error) {
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, false, mapContextError(
			ctx,
			"begin search-history prune batch",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &returnedErr)
	deleted, more, err := store.pruneScopeBatch(
		tx,
		scope,
		now,
		maximumRows,
	)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit().Error; err != nil {
		return 0, false, mapContextError(
			ctx,
			"commit search-history prune batch",
			err,
		)
	}
	return deleted, more, nil
}

func (store *Store) pruneScopeBatch(
	database *gorm.DB,
	scope AccessScope,
	now time.Time,
	maximumRows int,
) (int64, bool, error) {
	if maximumRows <= 0 {
		return 0, false, errors.New(
			"prune search history: batch size must be positive",
		)
	}

	countDeleted, _, err := store.deleteOldestExcess(
		database,
		scope,
		int64(maximumRows),
	)
	if err != nil {
		return 0, false, fmt.Errorf(
			"prune search history by count: %w",
			err,
		)
	}
	deleted := countDeleted
	remaining := int64(maximumRows) - deleted
	if remaining > 0 {
		cutoff := now.Add(-store.maximumAge).UnixMicro()
		expired := database.Model(&historyRecord{}).
			Select("search_job_id").
			Where(
				"tenant_id = ? AND owner_id = ? AND created_at_unix_micro < ?",
				scope.TenantID,
				scope.OwnerID,
				cutoff,
			).
			Order("created_at_unix_micro").
			Order("search_job_id").
			Limit(int(remaining))
		ageResult := database.
			Where(
				"tenant_id = ? AND owner_id = ? AND search_job_id IN (?)",
				scope.TenantID,
				scope.OwnerID,
				expired,
			).
			Delete(&historyRecord{})
		if ageResult.Error != nil {
			return 0, false, fmt.Errorf(
				"prune search history by age: %w",
				ageResult.Error,
			)
		}
		if ageResult.RowsAffected < 0 {
			return 0, false, errors.New(
				"prune search history by age: invalid affected row count",
			)
		}
		deleted += ageResult.RowsAffected
	}
	return deleted, deleted == int64(maximumRows), nil
}
