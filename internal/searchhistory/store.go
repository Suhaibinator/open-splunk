package searchhistory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
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
func (store *Store) Record(ctx context.Context, scope AccessScope, input *opensplunk.SearchHistoryEntry) (result *opensplunk.SearchHistoryEntry, returnedErr error) {
	return store.CompleteAttempt(ctx, scope, input)
}

// Get returns one detached owner-scoped terminal entry.
func (store *Store) Get(ctx context.Context, scope AccessScope, searchJobID string) (*opensplunk.SearchHistoryEntry, error) {
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
	query := resourcepolicy.Filter(applyHistoryScope(store.orm.WithContext(ctx), scope), "search_history").
		Where(
			"search_job_id = ? AND created_at_unix_micro >= ?",
			searchJobID, now.Add(-store.maximumAge).UnixMicro(),
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
// retention predicate instead of acquiring a PostgreSQL write lock.
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
func (store *Store) Delete(ctx context.Context, scope AccessScope, searchJobID string) (returnedErr error) {
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
	tx := pgmeta.Begin(store.orm.WithContext(ctx))
	if tx.Error != nil {
		return mapContextError(ctx, "begin search-history deletion", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	var identity historyRecord
	read := applyHistoryScope(tx, scope).Select("search_job_id", "tenant_id", "owner_id").Where("search_job_id = ?", searchJobID).Take(&identity)
	if errors.Is(read.Error, gorm.ErrRecordNotFound) {
		return control.ErrNotFound
	}
	if read.Error != nil {
		return mapContextError(ctx, "read search-history deletion owner", read.Error)
	}
	if err := lockHistoryOwners(tx, []historyRecord{identity}); err != nil {
		return mapContextError(ctx, "lock search-history deletion owner", err)
	}
	if err := lockHistoryJobs(tx, []historyRecord{identity}); err != nil {
		return mapContextError(ctx, "lock search-history deletion job", err)
	}
	result := applyHistoryScope(tx, scope).
		Where("search_job_id = ?", searchJobID).
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
	if err := tx.Commit().Error; err != nil {
		return mapContextError(ctx, "commit search-history deletion", err)
	}
	return nil
}

// Clear deletes every entry matching an owner-scoped filter and returns the
// exact affected row count. Confirmation policy belongs to the HTTP adapter.
func (store *Store) Clear(ctx context.Context, scope AccessScope, filter Filter) (deleted uint64, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	normalized, err := normalizeFilter(scope, filter)
	if err != nil {
		return 0, err
	}
	tx := pgmeta.Begin(store.orm.WithContext(ctx))
	if tx.Error != nil {
		return 0, mapContextError(ctx, "begin search-history clear", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	// Site administrators can clear multiple creators. Discover only owner
	// identities, acquire them in stable order, then keep that owner set fixed.
	var owners []historyRecord
	read := applyHistoryFilter(tx.Model(&historyRecord{}), normalized).Select("tenant_id", "owner_id").Distinct().Find(&owners)
	if read.Error != nil {
		return 0, mapContextError(ctx, "select search-history clear owners", read.Error)
	}
	if err := lockHistoryOwners(tx, owners); err != nil {
		return 0, mapContextError(ctx, "lock search-history clear owners", err)
	}
	var affected int64
	for _, owner := range owners {
		for {
			var candidates []historyRecord
			selection := applyHistoryFilter(tx.Model(&historyRecord{}), normalized).
				Where("tenant_id = ? AND owner_id = ?", owner.TenantID, owner.OwnerID).
				Select("search_job_id").Order("search_job_id").Limit(retentionPruneBatchSize).Find(&candidates)
			if selection.Error != nil {
				return 0, mapContextError(ctx, "select search-history clear jobs", selection.Error)
			}
			if len(candidates) == 0 {
				break
			}
			if err := lockHistoryJobs(tx, candidates); err != nil {
				return 0, mapContextError(ctx, "lock search-history clear jobs", err)
			}
			ids := make([]string, len(candidates))
			for index, candidate := range candidates {
				ids[index] = candidate.SearchJobID
			}
			result := applyHistoryFilter(tx, normalized).Where("search_job_id IN ?", ids).Delete(&historyRecord{})
			if result.Error != nil {
				return 0, mapContextError(ctx, "clear search history", result.Error)
			}
			if result.RowsAffected <= 0 || result.RowsAffected > int64(len(candidates)) {
				return 0, errors.New("clear search history: database changed an unexpected number of rows")
			}
			affected += result.RowsAffected
		}
	}
	if affected < 0 {
		return 0, errors.New("clear search history: database returned a negative row count")
	}
	if err := tx.Commit().Error; err != nil {
		return 0, mapContextError(ctx, "commit search-history clear", err)
	}
	return uint64(affected), nil
}

// The owner boundary precedes every history row/counter mutation. A row trigger
// alone cannot establish that order because PostgreSQL locks an UPDATE/DELETE
// target before executing its BEFORE ROW triggers.
func lockHistoryOwners(tx *gorm.DB, records []historyRecord) error {
	keys := make([]string, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		key := "search-history:" + record.TenantID + ":" + record.OwnerID
		if _, exists := seen[key]; !exists {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	return pgmeta.Lock(tx, keys...)
}

func lockHistoryJobs(tx *gorm.DB, records []historyRecord) error {
	keys := make([]string, len(records))
	for index, record := range records {
		keys[index] = "search-history-job:" + record.SearchJobID
	}
	return pgmeta.Lock(tx, keys...)
}

func historyEntryFromRecord(record historyRecord) (*opensplunk.SearchHistoryEntry, error) {
	if _, err := persistedScope(record.TenantID, record.OwnerID, record.OrganizationID); err != nil {
		return nil, persistedDataError("persisted search-history scope is invalid", err)
	}
	entry, indexed, err := decodeEntry(record.EntryProto, record.EntrySHA256)
	if err != nil {
		return nil, err
	}
	if !resourcepolicy.MatchesSourceNames(record.SourceIndexNames, indexed.sourceIndexNames) || indexed.jobID != record.SearchJobID || indexed.appID != record.AppID || indexed.savedSearchID != record.SavedSearchID ||
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

func cloneEntry(entry *opensplunk.SearchHistoryEntry) *opensplunk.SearchHistoryEntry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*opensplunk.SearchHistoryEntry)
}
