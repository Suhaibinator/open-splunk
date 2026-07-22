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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// New constructs a bounded history store over an already configured control
// database. Retention is enforced transactionally after every new record.
func New(database *control.DB, options Options) (*Store, error) {
	if database == nil || database.SQLDB() == nil {
		return nil, invalid("control database is required")
	}
	if len(options.CursorKey) < minimumCursorKeyBytes {
		return nil, invalid(fmt.Sprintf("cursor key must contain at least %d bytes", minimumCursorKeyBytes))
	}
	maximumAge := options.MaximumAge
	if maximumAge < 0 || maximumAge > hardMaximumAge {
		return nil, invalid(fmt.Sprintf("maximum age must be between zero and %s", hardMaximumAge))
	}
	if maximumAge == 0 {
		maximumAge = defaultMaximumAge
	}
	maximumEntries := options.MaximumEntriesPerOwner
	if maximumEntries < 0 || maximumEntries > hardMaximumEntriesPerOwner {
		return nil, invalid(fmt.Sprintf("maximum entries per owner must be between zero and %d", hardMaximumEntriesPerOwner))
	}
	if maximumEntries == 0 {
		maximumEntries = defaultMaximumEntriesPerOwner
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Store{
		db: database.SQLDB(), clock: clock, cursorKey: slices.Clone(options.CursorKey),
		maximumAge: maximumAge, maximumEntriesPerOwner: maximumEntries,
	}, nil
}

// Record atomically inserts one immutable terminal search snapshot and prunes
// that owner's history to its configured age and row-count bounds. Retrying an
// identical terminal callback is idempotent; different content for an existing
// search ID returns ErrVersionConflict instead of rewriting audit metadata.
func (store *Store) Record(ctx context.Context, scope AccessScope, input *opensplunkv1.SearchHistoryEntry) (result *opensplunkv1.SearchHistoryEntry, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	entry, indexed, err := normalizeEntry(input)
	if err != nil {
		return nil, err
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return nil, errors.New("record search history: clock returned an invalid timestamp")
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapContextError(ctx, "begin search-history record", err)
	}
	defer finishTx(tx, &returnedErr)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO search_history (
			search_job_id, tenant_id, owner_id, app_id, saved_search_id,
			final_state, search_text, created_at_unix_micro,
			finished_at_unix_micro, duration_nanoseconds, matched_events,
			entry_proto, entry_sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		indexed.jobID, scope.TenantID, scope.OwnerID, indexed.appID,
		indexed.savedSearchID, indexed.state, indexed.searchText,
		indexed.createdAt, indexed.finishedAt, indexed.duration,
		indexed.matchedEvents, indexed.encoded, indexed.checksum[:],
	)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("record search history: %w", contextErr)
		}
		var existingTenant, existingOwner string
		var existingEncoded, existingChecksum []byte
		lookupErr := tx.QueryRowContext(ctx, `
			SELECT tenant_id, owner_id, entry_proto, entry_sha256
			FROM search_history WHERE search_job_id = ?`, indexed.jobID,
		).Scan(&existingTenant, &existingOwner, &existingEncoded, &existingChecksum)
		if errors.Is(lookupErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("record search history: %w", err)
		}
		if lookupErr != nil {
			return nil, fmt.Errorf("classify duplicate search-history record: %w", lookupErr)
		}
		if existingTenant != scope.TenantID || existingOwner != scope.OwnerID {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if _, _, decodeErr := decodeEntry(existingEncoded, existingChecksum); decodeErr != nil {
			return nil, fmt.Errorf("read duplicate search-history record: %w", decodeErr)
		}
		if !slices.Equal(existingEncoded, indexed.encoded) || !slices.Equal(existingChecksum, indexed.checksum[:]) {
			return nil, control.ErrVersionConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent search-history record: %w", err)
		}
		return entry, nil
	}

	if _, err := store.pruneScope(ctx, tx, scope, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit search-history record: %w", err)
	}
	return entry, nil
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
	entry, err := scanHistoryEntry(store.db.QueryRowContext(ctx,
		historySelect+` WHERE search_job_id = ? AND tenant_id = ? AND owner_id = ? AND created_at_unix_micro >= ?`,
		searchJobID, scope.TenantID, scope.OwnerID, now.Add(-store.maximumAge).UnixMicro(),
	))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		return nil, mapContextError(ctx, "get search-history entry", err)
	}
	return entry, nil
}

// Prune applies both age and row-count retention for one owner immediately.
// Record calls it automatically; it is also exposed so runtime maintenance can
// reclaim expired disk rows while an owner is otherwise idle. Read paths apply
// a non-mutating retention predicate instead of acquiring a SQLite write lock.
func (store *Store) Prune(ctx context.Context, scope AccessScope) (deleted uint64, returnedErr error) {
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, mapContextError(ctx, "begin search-history prune", err)
	}
	defer finishTx(tx, &returnedErr)
	deleted, err = store.pruneScope(ctx, tx, scope, now)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit search-history prune: %w", err)
	}
	return deleted, nil
}

type contextExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (store *Store) pruneScope(ctx context.Context, executor contextExecutor, scope AccessScope, now time.Time) (uint64, error) {
	cutoff := now.Add(-store.maximumAge).UnixMicro()
	ageResult, err := executor.ExecContext(ctx, `
		DELETE FROM search_history
		WHERE tenant_id = ? AND owner_id = ? AND created_at_unix_micro < ?`,
		scope.TenantID, scope.OwnerID, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("prune search history by age: %w", err)
	}
	ageRows, err := ageResult.RowsAffected()
	if err != nil || ageRows < 0 {
		return 0, errors.New("prune search history by age: invalid affected row count")
	}
	countResult, err := executor.ExecContext(ctx, `
		DELETE FROM search_history
		WHERE search_job_id IN (
			SELECT search_job_id FROM search_history
			WHERE tenant_id = ? AND owner_id = ?
			ORDER BY created_at_unix_micro DESC, search_job_id DESC
			LIMIT -1 OFFSET ?
		)`, scope.TenantID, scope.OwnerID, store.maximumEntriesPerOwner,
	)
	if err != nil {
		return 0, fmt.Errorf("prune search history by count: %w", err)
	}
	countRows, err := countResult.RowsAffected()
	if err != nil || countRows < 0 {
		return 0, errors.New("prune search history by count: invalid affected row count")
	}
	return uint64(ageRows) + uint64(countRows), nil
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
	result, err := store.db.ExecContext(ctx, `
		DELETE FROM search_history
		WHERE search_job_id = ? AND tenant_id = ? AND owner_id = ?`,
		searchJobID, scope.TenantID, scope.OwnerID,
	)
	if err != nil {
		return mapContextError(ctx, "delete search-history entry", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted search-history row count: %w", err)
	}
	if rows == 0 {
		return control.ErrNotFound
	}
	if rows != 1 {
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
	query, args := filterQuery(`DELETE FROM search_history`, normalized)
	result, err := store.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, mapContextError(ctx, "clear search history", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read cleared search-history row count: %w", err)
	}
	if rows < 0 {
		return 0, errors.New("clear search history: database returned a negative row count")
	}
	return uint64(rows), nil
}

const historySelect = `
	SELECT search_job_id, tenant_id, owner_id, app_id, saved_search_id,
		final_state, search_text, created_at_unix_micro,
		finished_at_unix_micro, duration_nanoseconds, matched_events,
		entry_proto, entry_sha256
	FROM search_history`

type rowScanner interface {
	Scan(...any) error
}

func scanHistoryEntry(row rowScanner) (*opensplunkv1.SearchHistoryEntry, error) {
	var (
		jobID, tenantID, ownerID, appID, savedSearchID, searchText string
		state, createdAt, finishedAt, duration, matchedEvents      int64
		encoded, checksum                                          []byte
	)
	if err := row.Scan(
		&jobID, &tenantID, &ownerID, &appID, &savedSearchID,
		&state, &searchText, &createdAt, &finishedAt, &duration,
		&matchedEvents, &encoded, &checksum,
	); err != nil {
		return nil, err
	}
	if _, err := normalizeScope(AccessScope{TenantID: tenantID, OwnerID: ownerID}); err != nil {
		return nil, fmt.Errorf("persisted search-history scope is invalid: %v", err)
	}
	entry, indexed, err := decodeEntry(encoded, checksum)
	if err != nil {
		return nil, err
	}
	if indexed.jobID != jobID || indexed.appID != appID || indexed.savedSearchID != savedSearchID ||
		indexed.state != state || indexed.searchText != searchText || indexed.createdAt != createdAt ||
		indexed.finishedAt != finishedAt || indexed.duration != duration || indexed.matchedEvents != matchedEvents {
		return nil, errors.New("search-history indexed metadata does not match its canonical entry")
	}
	return entry, nil
}

func finishTx(tx *sql.Tx, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
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
