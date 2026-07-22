package searchhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrCapacity means an owner's bounded pending-attempt journal is full. It is
// deliberately separate from terminal retention: silently pruning an active
// attempt would make a crash-created gap in the audit trail.
var ErrCapacity = errors.New("search history pending capacity is exhausted")

type pendingIndexedEntry struct {
	jobID     string
	state     int64
	createdAt int64
	encoded   []byte
	checksum  [sha256.Size]byte
}

type pendingAttempt struct {
	scope   AccessScope
	entry   *opensplunkv1.SearchHistoryEntry
	indexed pendingIndexedEntry
}

// BeginAttempt durably admits a queued search before asynchronous parsing or
// execution begins. An exact retry is idempotent; a changed retry cannot
// rewrite the original search intent.
func (store *Store) BeginAttempt(ctx context.Context, scope AccessScope, input *opensplunkv1.SearchHistoryEntry) (result *opensplunkv1.SearchHistoryEntry, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	entry, indexed, err := normalizePendingEntry(input)
	if err != nil {
		return nil, err
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapContextError(ctx, "begin pending search-history record", err)
	}
	defer finishTx(tx, &returnedErr)

	var terminalTenant, terminalOwner string
	err = tx.QueryRowContext(ctx, `
		SELECT tenant_id, owner_id FROM search_history WHERE search_job_id = ?`,
		indexed.jobID,
	).Scan(&terminalTenant, &terminalOwner)
	switch {
	case err == nil:
		if _, scopeErr := normalizeScope(AccessScope{TenantID: terminalTenant, OwnerID: terminalOwner}); scopeErr != nil {
			return nil, fmt.Errorf("persisted terminal search-history scope is invalid: %v", scopeErr)
		}
		if terminalTenant != scope.TenantID || terminalOwner != scope.OwnerID {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		return nil, control.ErrVersionConflict
	case !errors.Is(err, sql.ErrNoRows):
		return nil, mapContextError(ctx, "check terminal search-history record", err)
	}

	existing, err := scanPendingAttempt(tx.QueryRowContext(ctx,
		pendingSelect+` WHERE search_job_id = ?`, indexed.jobID,
	))
	switch {
	case err == nil:
		if existing.scope != scope {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if !slices.Equal(existing.indexed.encoded, indexed.encoded) ||
			!slices.Equal(existing.indexed.checksum[:], indexed.checksum[:]) {
			return nil, control.ErrVersionConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent pending search-history record: %w", err)
		}
		return existing.entry, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, mapContextError(ctx, "read pending search-history record", err)
	}

	var pendingCount int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM search_history_pending
		WHERE tenant_id = ? AND owner_id = ?`, scope.TenantID, scope.OwnerID,
	).Scan(&pendingCount); err != nil {
		return nil, mapContextError(ctx, "count pending search-history records", err)
	}
	if pendingCount < 0 {
		return nil, errors.New("count pending search-history records: database returned a negative count")
	}
	if pendingCount >= int64(store.maximumEntriesPerOwner) {
		return nil, ErrCapacity
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO search_history_pending (
			search_job_id, tenant_id, owner_id, state,
			created_at_unix_micro, entry_proto, entry_sha256
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		indexed.jobID, scope.TenantID, scope.OwnerID, indexed.state,
		indexed.createdAt, indexed.encoded, indexed.checksum[:],
	); err != nil {
		return nil, mapContextError(ctx, "record pending search-history entry", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pending search-history record: %w", err)
	}
	return entry, nil
}

// CompleteAttempt atomically publishes a terminal entry and removes its
// pending journal row. It also accepts a terminal-only call for compatibility
// with synchronous callers of Record.
func (store *Store) CompleteAttempt(ctx context.Context, scope AccessScope, input *opensplunkv1.SearchHistoryEntry) (result *opensplunkv1.SearchHistoryEntry, returnedErr error) {
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
		return nil, errors.New("complete search history: clock returned an invalid timestamp")
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapContextError(ctx, "begin search-history completion", err)
	}
	defer finishTx(tx, &returnedErr)

	pending, err := scanPendingAttempt(tx.QueryRowContext(ctx,
		pendingSelect+` WHERE search_job_id = ?`, indexed.jobID,
	))
	hasPending := err == nil
	switch {
	case err == nil:
		if pending.scope != scope {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if !sameAdmission(pending.entry, entry) {
			return nil, control.ErrVersionConflict
		}
	case !errors.Is(err, sql.ErrNoRows):
		return nil, mapContextError(ctx, "read pending search-history completion", err)
	}

	if err := putTerminalEntry(ctx, tx, scope, indexed); err != nil {
		return nil, err
	}
	if hasPending {
		result, err := tx.ExecContext(ctx, `
			DELETE FROM search_history_pending
			WHERE search_job_id = ? AND tenant_id = ? AND owner_id = ?`,
			indexed.jobID, scope.TenantID, scope.OwnerID,
		)
		if err != nil {
			return nil, mapContextError(ctx, "remove completed pending search-history entry", err)
		}
		if err := requireOneAffected(result, "remove completed pending search-history entry"); err != nil {
			return nil, err
		}
	}
	if _, err := store.pruneScope(ctx, tx, scope, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit search-history completion: %w", err)
	}
	return entry, nil
}

// RecoverInterrupted turns every pending attempt for one owner into a safe,
// retryable terminal failure. It is intended to run during startup before the
// server accepts new search admissions.
func (store *Store) RecoverInterrupted(ctx context.Context, scope AccessScope) (recovered uint64, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return 0, err
	}
	now := store.clock().Round(0).UTC()
	if timestamppb.New(now).CheckValid() != nil {
		return 0, errors.New("recover search history: clock returned an invalid timestamp")
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, mapContextError(ctx, "begin interrupted search-history recovery", err)
	}
	defer finishTx(tx, &returnedErr)

	for {
		pending, scanErr := scanPendingAttempt(tx.QueryRowContext(ctx,
			pendingSelect+`
			 WHERE tenant_id = ? AND owner_id = ?
			 ORDER BY created_at_unix_micro, search_job_id
			 LIMIT 1`, scope.TenantID, scope.OwnerID,
		))
		if errors.Is(scanErr, sql.ErrNoRows) {
			break
		}
		if scanErr != nil {
			return 0, mapContextError(ctx, "read interrupted search-history entry", scanErr)
		}
		if pending.scope != scope {
			return 0, errors.New("pending search-history scope query returned a cross-scope entry")
		}

		terminal := cloneEntry(pending.entry)
		finished := now
		created := terminal.CreatedAt.AsTime()
		if created.After(finished) {
			finished = created
		}
		duration := time.Duration(0)
		if terminal.StartedAt != nil {
			started := terminal.StartedAt.AsTime()
			if started.After(finished) {
				finished = started
			}
			duration = finished.Sub(started)
		}
		terminal.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED
		terminal.FinishedAt = timestamppb.New(finished)
		terminal.Duration = durationpb.New(duration)
		terminal.Failure = &opensplunkv1.SearchFailure{
			Code:      opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL,
			Message:   "search interrupted by server restart",
			Retryable: true,
		}
		_, terminalIndexed, normalizeErr := normalizeEntry(terminal)
		if normalizeErr != nil {
			return 0, fmt.Errorf("finalize interrupted search-history entry: %w", normalizeErr)
		}
		if err := putTerminalEntry(ctx, tx, scope, terminalIndexed); err != nil {
			return 0, err
		}
		result, err := tx.ExecContext(ctx, `
			DELETE FROM search_history_pending
			WHERE search_job_id = ? AND tenant_id = ? AND owner_id = ?`,
			pending.indexed.jobID, scope.TenantID, scope.OwnerID,
		)
		if err != nil {
			return 0, mapContextError(ctx, "remove interrupted pending search-history entry", err)
		}
		if err := requireOneAffected(result, "remove interrupted pending search-history entry"); err != nil {
			return 0, err
		}
		recovered++
	}

	if _, err := store.pruneScope(ctx, tx, scope, now); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit interrupted search-history recovery: %w", err)
	}
	return recovered, nil
}

func normalizePendingEntry(input *opensplunkv1.SearchHistoryEntry) (*opensplunkv1.SearchHistoryEntry, pendingIndexedEntry, error) {
	if input == nil {
		return nil, pendingIndexedEntry{}, invalid("search-history entry is required")
	}
	entry := cloneEntry(input)
	if entry.FinalState != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED {
		return nil, pendingIndexedEntry{}, invalid("pending attempt state must be queued")
	}
	if entry.FinishedAt != nil || entry.Duration != nil || entry.Failure != nil || len(entry.Warnings) != 0 ||
		entry.MatchedEvents != 0 || entry.ScannedRows != 0 || entry.ScannedBytes != 0 || entry.ProducedRows != 0 {
		return nil, pendingIndexedEntry{}, invalid("pending attempt cannot contain terminal metadata")
	}

	// Reuse all terminal-entry validation and canonicalization with a temporary
	// canceled state. The synthetic finish only brackets an optional started_at;
	// it is removed before the pending entry is encoded.
	synthetic := cloneEntry(entry)
	synthetic.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED
	synthetic.FinishedAt = cloneTimestamp(synthetic.CreatedAt)
	if synthetic.StartedAt != nil && synthetic.StartedAt.CheckValid() == nil &&
		(synthetic.FinishedAt == nil || synthetic.FinishedAt.CheckValid() != nil || synthetic.StartedAt.AsTime().After(synthetic.FinishedAt.AsTime())) {
		synthetic.FinishedAt = cloneTimestamp(synthetic.StartedAt)
	}
	synthetic.Duration = durationpb.New(0)
	normalized, terminalIndexed, err := normalizeEntry(synthetic)
	if err != nil {
		return nil, pendingIndexedEntry{}, err
	}
	normalized.FinalState = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED
	normalized.FinishedAt = nil
	normalized.Duration = nil
	normalized.Failure = nil

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(normalized)
	if err != nil {
		return nil, pendingIndexedEntry{}, fmt.Errorf("encode pending search-history entry: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maximumEntryBytes {
		return nil, pendingIndexedEntry{}, invalid(fmt.Sprintf("search-history entry cannot exceed %d bytes", maximumEntryBytes))
	}
	indexed := pendingIndexedEntry{
		jobID: normalized.SearchJobId, state: int64(normalized.FinalState),
		createdAt: terminalIndexed.createdAt, encoded: encoded,
		checksum: sha256.Sum256(encoded),
	}
	return normalized, indexed, nil
}

func decodePendingEntry(encoded, expectedChecksum []byte) (*opensplunkv1.SearchHistoryEntry, pendingIndexedEntry, error) {
	checksum := sha256.Sum256(encoded)
	if len(expectedChecksum) != sha256.Size || !bytes.Equal(checksum[:], expectedChecksum) {
		return nil, pendingIndexedEntry{}, errors.New("pending search-history entry checksum mismatch")
	}
	entry := new(opensplunkv1.SearchHistoryEntry)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, entry); err != nil {
		return nil, pendingIndexedEntry{}, fmt.Errorf("decode pending search-history entry: %w", err)
	}
	normalizedEntry, normalized, err := normalizePendingEntry(entry)
	if err != nil {
		return nil, pendingIndexedEntry{}, fmt.Errorf("validate persisted pending search-history entry: %v", err)
	}
	if !bytes.Equal(normalized.encoded, encoded) {
		return nil, pendingIndexedEntry{}, errors.New("persisted pending search-history entry is not canonical")
	}
	return normalizedEntry, normalized, nil
}

const pendingSelect = `
	SELECT search_job_id, tenant_id, owner_id, state,
		created_at_unix_micro, entry_proto, entry_sha256
	FROM search_history_pending`

func scanPendingAttempt(row rowScanner) (*pendingAttempt, error) {
	var (
		jobID, tenantID, ownerID string
		state, createdAt         int64
		encoded, checksum        []byte
	)
	if err := row.Scan(&jobID, &tenantID, &ownerID, &state, &createdAt, &encoded, &checksum); err != nil {
		return nil, err
	}
	scope, err := normalizeScope(AccessScope{TenantID: tenantID, OwnerID: ownerID})
	if err != nil {
		return nil, fmt.Errorf("persisted pending search-history scope is invalid: %v", err)
	}
	entry, indexed, err := decodePendingEntry(encoded, checksum)
	if err != nil {
		return nil, err
	}
	if indexed.jobID != jobID || indexed.state != state || indexed.createdAt != createdAt {
		return nil, errors.New("pending search-history indexed metadata does not match its canonical entry")
	}
	return &pendingAttempt{scope: scope, entry: entry, indexed: indexed}, nil
}

func sameAdmission(pending, terminal *opensplunkv1.SearchHistoryEntry) bool {
	return pending.SearchJobId == terminal.SearchJobId &&
		proto.Equal(pending.Definition, terminal.Definition) &&
		proto.Equal(pending.Source, terminal.Source) &&
		proto.Equal(pending.ResolvedTimeRange, terminal.ResolvedTimeRange) &&
		pending.CompilerVersion == terminal.CompilerVersion &&
		proto.Equal(pending.CreatedAt, terminal.CreatedAt)
}

func putTerminalEntry(ctx context.Context, tx *sql.Tx, scope AccessScope, indexed indexedEntry) error {
	var existingTenant, existingOwner string
	var existingEncoded, existingChecksum []byte
	err := tx.QueryRowContext(ctx, `
		SELECT tenant_id, owner_id, entry_proto, entry_sha256
		FROM search_history WHERE search_job_id = ?`, indexed.jobID,
	).Scan(&existingTenant, &existingOwner, &existingEncoded, &existingChecksum)
	switch {
	case err == nil:
		if existingTenant != scope.TenantID || existingOwner != scope.OwnerID {
			return fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if _, _, decodeErr := decodeEntry(existingEncoded, existingChecksum); decodeErr != nil {
			return fmt.Errorf("read duplicate search-history record: %w", decodeErr)
		}
		if !slices.Equal(existingEncoded, indexed.encoded) || !slices.Equal(existingChecksum, indexed.checksum[:]) {
			return control.ErrVersionConflict
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return mapContextError(ctx, "check terminal search-history record", err)
	}

	if _, err := tx.ExecContext(ctx, `
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
	); err != nil {
		return mapContextError(ctx, "record terminal search-history entry", err)
	}
	return nil
}

func requireOneAffected(result sql.Result, operation string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: read affected row count: %w", operation, err)
	}
	if rows != 1 {
		return fmt.Errorf("%s: database changed %d rows, want 1", operation, rows)
	}
	return nil
}

func cloneTimestamp(value *timestamppb.Timestamp) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*timestamppb.Timestamp)
}
