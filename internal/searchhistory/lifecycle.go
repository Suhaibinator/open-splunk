package searchhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
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

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, mapContextError(ctx, "begin pending search-history record", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	var terminal historyRecord
	err = tx.Select("tenant_id", "owner_id").
		Where("search_job_id = ?", indexed.jobID).
		Take(&terminal).Error
	switch {
	case err == nil:
		if _, scopeErr := normalizeScope(AccessScope{TenantID: terminal.TenantID, OwnerID: terminal.OwnerID}); scopeErr != nil {
			return nil, persistedDataError("persisted terminal search-history scope is invalid", scopeErr)
		}
		if terminal.TenantID != scope.TenantID || terminal.OwnerID != scope.OwnerID {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		return nil, control.ErrVersionConflict
	case !errors.Is(err, gorm.ErrRecordNotFound):
		return nil, mapContextError(ctx, "check terminal search-history record", err)
	}

	var existingRecord pendingHistoryRecord
	err = tx.Where("search_job_id = ?", indexed.jobID).Take(&existingRecord).Error
	switch {
	case err == nil:
		existing, conversionErr := pendingAttemptFromRecord(existingRecord)
		if conversionErr != nil {
			return nil, mapContextError(ctx, "read pending search-history record", conversionErr)
		}
		if existing.scope != scope {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if !slices.Equal(existing.indexed.encoded, indexed.encoded) ||
			!slices.Equal(existing.indexed.checksum[:], indexed.checksum[:]) {
			return nil, control.ErrVersionConflict
		}
		if err := tx.Commit().Error; err != nil {
			return nil, fmt.Errorf("commit idempotent pending search-history record: %w", err)
		}
		return existing.entry, nil
	case !errors.Is(err, gorm.ErrRecordNotFound):
		return nil, mapContextError(ctx, "read pending search-history record", err)
	}

	var pendingCount int64
	count := tx.Model(&pendingHistoryRecord{}).
		Where("tenant_id = ? AND owner_id = ?", scope.TenantID, scope.OwnerID).
		Count(&pendingCount)
	if count.Error != nil {
		return nil, mapContextError(ctx, "count pending search-history records", count.Error)
	}
	if pendingCount < 0 {
		return nil, errors.New("count pending search-history records: database returned a negative count")
	}
	if pendingCount >= int64(store.maximumEntriesPerOwner) {
		return nil, ErrCapacity
	}

	create := tx.Create(&pendingHistoryRecord{
		SearchJobID:        indexed.jobID,
		TenantID:           scope.TenantID,
		OwnerID:            scope.OwnerID,
		State:              indexed.state,
		CreatedAtUnixMicro: indexed.createdAt,
		EntryProto:         slices.Clone(indexed.encoded),
		EntrySHA256:        slices.Clone(indexed.checksum[:]),
	})
	if create.Error != nil {
		return nil, mapContextError(ctx, "record pending search-history entry", create.Error)
	}
	if err := tx.Commit().Error; err != nil {
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

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, mapContextError(ctx, "begin search-history completion", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	var pendingRecord pendingHistoryRecord
	err = tx.Where("search_job_id = ?", indexed.jobID).Take(&pendingRecord).Error
	hasPending := err == nil
	switch {
	case err == nil:
		pending, conversionErr := pendingAttemptFromRecord(pendingRecord)
		if conversionErr != nil {
			return nil, mapContextError(ctx, "read pending search-history completion", conversionErr)
		}
		if pending.scope != scope {
			return nil, fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if !sameAdmission(pending.entry, entry) {
			return nil, control.ErrVersionConflict
		}
	case !errors.Is(err, gorm.ErrRecordNotFound):
		return nil, mapContextError(ctx, "read pending search-history completion", err)
	}

	if err := putTerminalEntry(ctx, tx, scope, indexed); err != nil {
		return nil, err
	}
	if hasPending {
		result := tx.
			Where("search_job_id = ? AND tenant_id = ? AND owner_id = ?", indexed.jobID, scope.TenantID, scope.OwnerID).
			Delete(&pendingHistoryRecord{})
		if result.Error != nil {
			return nil, mapContextError(ctx, "remove completed pending search-history entry", result.Error)
		}
		if err := requireOneAffected(result.RowsAffected, "remove completed pending search-history entry"); err != nil {
			return nil, err
		}
	}
	if _, err := store.pruneScope(tx, scope, now); err != nil {
		return nil, err
	}
	if err := tx.Commit().Error; err != nil {
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

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, mapContextError(ctx, "begin interrupted search-history recovery", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	for {
		var pendingRecord pendingHistoryRecord
		read := tx.
			Where("tenant_id = ? AND owner_id = ?", scope.TenantID, scope.OwnerID).
			Order("created_at_unix_micro").
			Order("search_job_id").
			Take(&pendingRecord)
		if errors.Is(read.Error, gorm.ErrRecordNotFound) {
			break
		}
		if read.Error != nil {
			return 0, mapContextError(ctx, "read interrupted search-history entry", read.Error)
		}
		pending, conversionErr := pendingAttemptFromRecord(pendingRecord)
		if conversionErr != nil {
			return 0, mapContextError(ctx, "read interrupted search-history entry", conversionErr)
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
			return 0, persistedDataError("finalize interrupted search-history entry", normalizeErr)
		}
		if err := putTerminalEntry(ctx, tx, scope, terminalIndexed); err != nil {
			return 0, err
		}
		result := tx.
			Where(
				"search_job_id = ? AND tenant_id = ? AND owner_id = ?",
				pending.indexed.jobID, scope.TenantID, scope.OwnerID,
			).
			Delete(&pendingHistoryRecord{})
		if result.Error != nil {
			return 0, mapContextError(ctx, "remove interrupted pending search-history entry", result.Error)
		}
		if err := requireOneAffected(result.RowsAffected, "remove interrupted pending search-history entry"); err != nil {
			return 0, err
		}
		recovered++
	}

	if _, err := store.pruneScope(tx, scope, now); err != nil {
		return 0, err
	}
	if err := tx.Commit().Error; err != nil {
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
		return nil, pendingIndexedEntry{}, persistedDataError("validate persisted pending search-history entry", err)
	}
	if !bytes.Equal(normalized.encoded, encoded) {
		return nil, pendingIndexedEntry{}, errors.New("persisted pending search-history entry is not canonical")
	}
	return normalizedEntry, normalized, nil
}

func pendingAttemptFromRecord(record pendingHistoryRecord) (*pendingAttempt, error) {
	scope, err := normalizeScope(AccessScope{TenantID: record.TenantID, OwnerID: record.OwnerID})
	if err != nil {
		return nil, persistedDataError("persisted pending search-history scope is invalid", err)
	}
	entry, indexed, err := decodePendingEntry(record.EntryProto, record.EntrySHA256)
	if err != nil {
		return nil, err
	}
	if indexed.jobID != record.SearchJobID || indexed.state != record.State ||
		indexed.createdAt != record.CreatedAtUnixMicro {
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

func putTerminalEntry(ctx context.Context, tx *gorm.DB, scope AccessScope, indexed indexedEntry) error {
	var existing historyRecord
	err := tx.
		Select("tenant_id", "owner_id", "entry_proto", "entry_sha256").
		Where("search_job_id = ?", indexed.jobID).
		Take(&existing).Error
	switch {
	case err == nil:
		if existing.TenantID != scope.TenantID || existing.OwnerID != scope.OwnerID {
			return fmt.Errorf("%w: search job ID already exists", control.ErrAlreadyExists)
		}
		if _, _, decodeErr := decodeEntry(existing.EntryProto, existing.EntrySHA256); decodeErr != nil {
			return fmt.Errorf("read duplicate search-history record: %w", decodeErr)
		}
		if !slices.Equal(existing.EntryProto, indexed.encoded) ||
			!slices.Equal(existing.EntrySHA256, indexed.checksum[:]) {
			return control.ErrVersionConflict
		}
		return nil
	case !errors.Is(err, gorm.ErrRecordNotFound):
		return mapContextError(ctx, "check terminal search-history record", err)
	}

	create := tx.Create(&historyRecord{
		SearchJobID:         indexed.jobID,
		TenantID:            scope.TenantID,
		OwnerID:             scope.OwnerID,
		AppID:               indexed.appID,
		SavedSearchID:       indexed.savedSearchID,
		FinalState:          indexed.state,
		SearchText:          indexed.searchText,
		CreatedAtUnixMicro:  indexed.createdAt,
		FinishedAtUnixMicro: indexed.finishedAt,
		DurationNanoseconds: indexed.duration,
		MatchedEvents:       indexed.matchedEvents,
		EntryProto:          slices.Clone(indexed.encoded),
		EntrySHA256:         slices.Clone(indexed.checksum[:]),
	})
	if create.Error != nil {
		return mapContextError(ctx, "record terminal search-history entry", create.Error)
	}
	return nil
}

func requireOneAffected(rows int64, operation string) error {
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
