package searchhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

// New constructs a bounded history store over an already configured control
// database. Terminal writes enforce ordinary per-owner count growth and
// advance age cleanup transactionally; the database owner must also schedule
// PruneMaintenanceBatch so scopes remain physically bounded while idle.
func New(database *control.DB, options Options) (*Store, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, invalid("control database is required")
	}
	if len(options.CursorKey) < minimumCursorKeyBytes {
		return nil, invalid(fmt.Sprintf("cursor key must contain at least %d bytes", minimumCursorKeyBytes))
	}
	if options.AuditAppender != nil && nilcheck.IsNil(options.AuditAppender) {
		return nil, invalid("search-attempt audit appender is nil")
	}
	if options.RequireSearchAttemptAudit && options.AuditAppender == nil {
		return nil, invalid("search-attempt audit appender is required")
	}
	retention, err := ResolveRetentionPolicy(
		options.MaximumAge,
		options.MaximumEntriesPerOwner,
	)
	if err != nil {
		return nil, err
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Store{
		orm: database.GORMDB(), clock: clock, cursorKey: slices.Clone(options.CursorKey),
		maximumAge: retention.MaximumAge, maximumEntriesPerOwner: retention.MaximumEntriesPerOwner,
		searchAttemptAuditAppender: options.AuditAppender,
	}, nil
}

// Record atomically inserts one immutable terminal search snapshot and advances
// bounded physical retention for that owner. Normal sequential growth remains
// count-bounded; maintenance completes any inherited backlog. Retrying an
// identical terminal callback is idempotent; different content for an existing
// search ID returns ErrVersionConflict instead of rewriting audit metadata.
func (store *Store) Record(ctx context.Context, scope AccessScope, input *opensplunkv1.SearchHistoryEntry) (result *opensplunkv1.SearchHistoryEntry, returnedErr error) {
	return store.CompleteAttempt(ctx, scope, input)
}

// Get returns one detached owner-scoped terminal entry.
func (store *Store) Get(ctx context.Context, scope AccessScope, searchJobID string) (*opensplunkv1.SearchHistoryEntry, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	searchJobID = strings.TrimSpace(searchJobID)
	if err := validateText("search job ID", searchJobID, maximumSearchJobIDBytes, false); err != nil {
		return nil, err
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return nil, errors.New("get search history: clock returned an invalid timestamp")
	}
	var record historyRecord
	query := store.orm.WithContext(ctx).
		Where(
			"search_job_id = ? AND tenant_id = ? AND owner_id = ? AND created_at_unix_micro >= ?",
			searchJobID, scope.TenantID, scope.OwnerID, now.Add(-store.maximumAge).UnixMicro(),
		).
		Take(&record)
	if errors.Is(query.Error, gorm.ErrRecordNotFound) {
		return nil, control.ErrNotFound
	}
	if query.Error != nil {
		return nil, mapContextError(ctx, "get search-history entry", query.Error)
	}
	entry, err := historyEntryFromRecord(record)
	if err != nil {
		return nil, mapContextError(ctx, "get search-history entry", err)
	}
	return entry, nil
}

// Prune applies both age and row-count retention for one owner immediately.
// Each transaction deletes only a fixed-size batch so other control-plane
// writers can make progress between batches. Read paths apply a non-mutating
// retention predicate instead of acquiring a SQLite write lock.
func (store *Store) Prune(ctx context.Context, scope AccessScope) (deleted int64, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return 0, err
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return 0, errors.New("prune search history: clock returned an invalid timestamp")
	}
	for {
		batchDeleted, more, err := store.pruneScopeTransaction(
			ctx,
			scope,
			now,
			retentionPruneBatchSize,
		)
		deleted += batchDeleted
		if err != nil {
			return deleted, err
		}
		if !more {
			return deleted, nil
		}
	}
}

// Delete removes one entry without disclosing cross-scope existence.
func (store *Store) Delete(ctx context.Context, scope AccessScope, searchJobID string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return err
	}
	searchJobID = strings.TrimSpace(searchJobID)
	if err := validateText("search job ID", searchJobID, maximumSearchJobIDBytes, false); err != nil {
		return err
	}
	result := store.orm.WithContext(ctx).
		Where("search_job_id = ? AND tenant_id = ? AND owner_id = ?", searchJobID, scope.TenantID, scope.OwnerID).
		Delete(&historyRecord{})
	if result.Error != nil {
		return mapContextError(ctx, "delete search-history entry", result.Error)
	}
	if result.RowsAffected == 0 {
		return control.ErrNotFound
	}
	if result.RowsAffected != 1 {
		return errors.New("delete search-history entry: database changed an unexpected number of rows")
	}
	return nil
}

// Clear deletes every entry matching an owner-scoped filter and returns the
// exact affected row count. Confirmation policy belongs to the HTTP adapter.
func (store *Store) Clear(ctx context.Context, scope AccessScope, filter Filter) (uint64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	normalized, err := normalizeFilter(scope, filter)
	if err != nil {
		return 0, err
	}
	result := applyHistoryFilter(store.orm.WithContext(ctx), normalized).Delete(&historyRecord{})
	if result.Error != nil {
		return 0, mapContextError(ctx, "clear search history", result.Error)
	}
	if result.RowsAffected < 0 {
		return 0, errors.New("clear search history: database returned a negative row count")
	}
	return uint64(result.RowsAffected), nil
}

func historyEntryFromRecord(record historyRecord) (*opensplunkv1.SearchHistoryEntry, error) {
	if _, err := normalizeScope(AccessScope{TenantID: record.TenantID, OwnerID: record.OwnerID}); err != nil {
		return nil, persistedDataError("persisted search-history scope is invalid", err)
	}
	entry, indexed, err := decodeEntry(record.EntryProto, record.EntrySHA256)
	if err != nil {
		return nil, err
	}
	if indexed.jobID != record.SearchJobID || indexed.appID != record.AppID || indexed.savedSearchID != record.SavedSearchID ||
		indexed.state != record.FinalState || indexed.searchText != record.SearchText ||
		indexed.createdAt != record.CreatedAtUnixMicro || indexed.finishedAt != record.FinishedAtUnixMicro ||
		indexed.duration != record.DurationNanoseconds || indexed.matchedEvents != record.MatchedEvents {
		return nil, errors.New("search-history indexed metadata does not match its canonical entry")
	}
	return entry, nil
}

func finishGORMTx(tx *gorm.DB, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
		*returnedErr = errors.Join(*returnedErr, fmt.Errorf("roll back search-history transaction: %w", err))
	}
}

func mapContextError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return fmt.Errorf("%s: %w", operation, contextErr)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func cloneEntry(entry *opensplunkv1.SearchHistoryEntry) *opensplunkv1.SearchHistoryEntry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*opensplunkv1.SearchHistoryEntry)
}
