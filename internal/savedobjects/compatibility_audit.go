package savedobjects

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const MaximumCompatibilityAuditSavedSearches = 1_000_000

// ErrCompatibilityAuditLimitExceeded reports that the bounded audit inventory
// contains more saved searches than one process may inspect.
var ErrCompatibilityAuditLimitExceeded = errors.New("saved-search compatibility audit limit exceeded")

// ScanAllForCompatibilityAudit streams every saved search in deterministic
// owner/name/ID order through one bounded SELECT. Each durable record passes
// through the same complete decoder as Get and List before visit runs.
//
// SPL is exposed only as a callback argument and is never retained in or
// returned from this API. Callers must apply the same rule to callback state.
func (store *Store) ScanAllForCompatibilityAudit(
	ctx context.Context,
	visit func(savedSearchID, spl string) error,
) (uint64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if store == nil || store.orm == nil {
		return 0, fmt.Errorf("%w: saved-search store is required", control.ErrInvalidArgument)
	}
	if visit == nil {
		return 0, fmt.Errorf("%w: compatibility audit visitor is required", control.ErrInvalidArgument)
	}

	query := store.orm.WithContext(ctx).
		Model(&savedSearchRecord{}).
		Order("owner_id ASC, name ASC, saved_search_id ASC").
		Limit(MaximumCompatibilityAuditSavedSearches + 1)
	rows, err := query.Rows()
	if err != nil {
		return 0, mapContextError(ctx, "scan saved searches for compatibility audit", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = rows.Close()
		}
	}()

	var scanned uint64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return scanned, err
		}
		var record savedSearchRecord
		if err := query.ScanRows(rows, &record); err != nil {
			return scanned, mapContextError(
				ctx,
				"scan saved search for compatibility audit",
				err,
			)
		}
		savedSearch, err := savedSearchFromRecord(record)
		if err != nil {
			return scanned, fmt.Errorf("scan saved search for compatibility audit: %w", err)
		}
		if scanned == MaximumCompatibilityAuditSavedSearches {
			return scanned, fmt.Errorf(
				"%w: maximum is %d",
				ErrCompatibilityAuditLimitExceeded,
				MaximumCompatibilityAuditSavedSearches,
			)
		}
		if err := visit(
			savedSearch.GetSavedSearchId(),
			savedSearch.GetDefinition().GetSearch().GetSpl(),
		); err != nil {
			return scanned, err
		}
		scanned++
	}
	if err := rows.Err(); err != nil {
		return scanned, mapContextError(ctx, "scan saved searches for compatibility audit", err)
	}
	if err := ctx.Err(); err != nil {
		return scanned, err
	}
	if err := rows.Close(); err != nil {
		return scanned, fmt.Errorf("close saved-search compatibility audit rows: %w", err)
	}
	closed = true
	return scanned, nil
}
