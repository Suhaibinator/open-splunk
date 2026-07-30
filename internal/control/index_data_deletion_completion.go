package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"gorm.io/gorm"
)

// IndexDataDeletionCompletion is the immutable terminal audit for one
// physically deleted index generation. The underlying index row remains in
// its deleting generation so existing foreign keys and the canonical name
// stay reserved; the associated tombstone hides it from the live catalog.
type IndexDataDeletionCompletion struct {
	DeletionOperationID string
	CorrelationID       string
	IndexID             string
	IndexName           string
	ArchivedVersion     uint64
	DeletedVersion      uint64
	Target              IndexDeletionMutationTarget
	ProtocolVersion     uint32
	OperationCreatedAt  time.Time
	MutationCreatedAt   time.Time
	CompletedAt         time.Time
}

// CompleteIndexDataDeletion atomically records terminal physical-deletion
// audit state, creates the catalog tombstone, and consumes the outstanding
// operation and mutation attempt. expected must be the exact durable attempt
// whose ClickHouse request was just proven physically empty while writes were
// frozen. Exact concurrent and restart retries return the same completion.
func (db *DB) CompleteIndexDataDeletion(
	ctx context.Context,
	expected IndexDeletionMutationAttempt,
) (result IndexDataDeletionCompletion, err error) {
	if err := validateExpectedIndexDeletionMutationAttempt(expected); err != nil {
		return IndexDataDeletionCompletion{}, err
	}

	database := db.orm.WithContext(ctx)
	existing, found, readErr := takeIndexDataDeletionCompletion(
		database,
		expected.DeletionOperationID,
	)
	if readErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read existing index data deletion completion: %w",
			readErr,
		)
	}
	if found {
		return matchingIndexDataDeletionCompletion(
			database,
			existing,
			expected,
		)
	}

	tx := database.Begin()
	if tx.Error != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"begin index data deletion completion: %w",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &err)

	existing, found, readErr = takeIndexDataDeletionCompletion(
		tx,
		expected.DeletionOperationID,
	)
	if readErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read existing index data deletion completion: %w",
			readErr,
		)
	}
	if found {
		completion, conversionErr := matchingIndexDataDeletionCompletion(
			tx,
			existing,
			expected,
		)
		if conversionErr != nil {
			return IndexDataDeletionCompletion{}, conversionErr
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			return IndexDataDeletionCompletion{}, fmt.Errorf(
				"commit idempotent index data deletion completion: %w",
				commitErr,
			)
		}
		return completion, nil
	}

	attemptRecord, found, readErr := takeIndexDeletionMutationAttempt(
		tx,
		expected.DeletionOperationID,
	)
	if readErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read index deletion mutation attempt for completion: %w",
			readErr,
		)
	}
	if !found {
		return IndexDataDeletionCompletion{}, ErrNotFound
	}
	attempt, operation, conversionErr := validatedIndexDeletionMutationAttemptWithOperation(
		tx,
		attemptRecord,
	)
	if conversionErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read index deletion mutation attempt for completion: %w",
			conversionErr,
		)
	}
	if !sameIndexDeletionMutationAttempt(attempt, expected) {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"%w: index deletion mutation attempt changed",
			ErrDependencyConflict,
		)
	}

	completedAt := databaseTime(time.Now())
	if completedAt.Before(attempt.CreatedAt) {
		completedAt = attempt.CreatedAt
	}
	record := indexDataDeletionCompletionRecord{
		DeletionOperationID: operation.ID,
		CorrelationID:       attempt.CorrelationID,
		IndexID:             operation.IndexID,
		IndexName:           operation.IndexName,
		// #nosec G115 -- operation decoding bounds both versions to SQLite int64.
		ArchivedIndexVersion: int64(operation.ArchivedVersion),
		// #nosec G115 -- operation decoding bounds both versions to SQLite int64.
		DeletingIndexVersion:        int64(operation.DeletingVersion),
		TenantID:                    attempt.Target.TenantID,
		ClickHouseDatabase:          attempt.Target.Database,
		ClickHouseTable:             attempt.Target.Table,
		ClickHouseTableUUID:         attempt.Target.TableUUID,
		ProtocolVersion:             int64(attempt.ProtocolVersion),
		OperationCreatedAtUnixMicro: operation.CreatedAt.UnixMicro(),
		AttemptCreatedAtUnixMicro:   attempt.CreatedAt.UnixMicro(),
		CompletedAtUnixMicro:        completedAt.UnixMicro(),
	}
	if create := tx.Create(&record); create.Error != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"create index data deletion completion: %w",
			create.Error,
		)
	}
	completion, conversionErr := validatedIndexDataDeletionCompletion(tx, record)
	if conversionErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read created index data deletion completion: %w",
			conversionErr,
		)
	}
	if !IndexDataDeletionCompletionMatchesAttempt(completion, expected) {
		return IndexDataDeletionCompletion{}, errors.New(
			"created index data deletion completion does not match expected attempt",
		)
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"commit index data deletion completion: %w",
			commitErr,
		)
	}
	return completion, nil
}

// GetIndexDataDeletionCompletion returns the immutable terminal audit for one
// deletion operation.
func (db *DB) GetIndexDataDeletionCompletion(
	ctx context.Context,
	deletionOperationID string,
) (IndexDataDeletionCompletion, error) {
	if !protocolid.Valid(deletionOperationID) {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"%w: index deletion operation ID is invalid",
			ErrInvalidArgument,
		)
	}
	record, found, err := takeIndexDataDeletionCompletion(
		db.orm.WithContext(ctx),
		deletionOperationID,
	)
	if err != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"get index data deletion completion: %w",
			err,
		)
	}
	if !found {
		return IndexDataDeletionCompletion{}, ErrNotFound
	}
	completion, err := validatedIndexDataDeletionCompletion(
		db.orm.WithContext(ctx),
		record,
	)
	if err != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"get index data deletion completion: %w",
			err,
		)
	}
	return completion, nil
}

func takeIndexDataDeletionCompletion(
	database *gorm.DB,
	deletionOperationID string,
) (indexDataDeletionCompletionRecord, bool, error) {
	var record indexDataDeletionCompletionRecord
	result := database.
		Where("deletion_operation_id = ?", deletionOperationID).
		Take(&record)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return indexDataDeletionCompletionRecord{}, false, nil
	}
	if result.Error != nil {
		return indexDataDeletionCompletionRecord{}, false, result.Error
	}
	return record, true, nil
}

func matchingIndexDataDeletionCompletion(
	database *gorm.DB,
	record indexDataDeletionCompletionRecord,
	expected IndexDeletionMutationAttempt,
) (IndexDataDeletionCompletion, error) {
	completion, err := validatedIndexDataDeletionCompletion(database, record)
	if err != nil {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"read existing index data deletion completion: %w",
			err,
		)
	}
	if !IndexDataDeletionCompletionMatchesAttempt(completion, expected) {
		return IndexDataDeletionCompletion{}, fmt.Errorf(
			"%w: completed index deletion mutation attempt changed",
			ErrDependencyConflict,
		)
	}
	return completion, nil
}

func validatedIndexDataDeletionCompletion(
	database *gorm.DB,
	record indexDataDeletionCompletionRecord,
) (IndexDataDeletionCompletion, error) {
	completion, err := indexDataDeletionCompletionFromRecord(record)
	if err != nil {
		return IndexDataDeletionCompletion{}, err
	}

	var relationship struct {
		Matched int `gorm:"column:matched"`
	}
	result := database.
		Table("indexes AS retained_index").
		Select("1 AS matched").
		Joins(`
			JOIN index_deletion_tombstones AS tombstone
			  ON tombstone.index_id = retained_index.index_id
			 AND tombstone.name = retained_index.name
			 AND tombstone.deleted_version = retained_index.version`).
		Where(`
			retained_index.index_id = ?
			AND retained_index.name = ?
			AND retained_index.version = ?
			AND retained_index.state = ?
			AND retained_index.updated_at_unix_micro = ?
			AND tombstone.deleted_at_unix_micro = ?
			AND NOT EXISTS (
				SELECT 1
				FROM index_deletion_operations
				WHERE deletion_operation_id = ?
			)
			AND NOT EXISTS (
				SELECT 1
				FROM index_deletion_mutation_attempts
				WHERE deletion_operation_id = ?
			)`,
			completion.IndexID,
			completion.IndexName,
			record.DeletingIndexVersion,
			IndexStateDeleting,
			completion.OperationCreatedAt.UnixMicro(),
			completion.CompletedAt.UnixMicro(),
			completion.DeletionOperationID,
			completion.DeletionOperationID,
		).
		Take(&relationship)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return IndexDataDeletionCompletion{},
			invalidIndexDataDeletionCompletion()
	}
	if result.Error != nil {
		return IndexDataDeletionCompletion{}, result.Error
	}
	return completion, nil
}

func indexDataDeletionCompletionFromRecord(
	record indexDataDeletionCompletionRecord,
) (IndexDataDeletionCompletion, error) {
	operation, operationErr := indexDeletionOperationFromRecord(
		indexDeletionOperationRecord{
			DeletionOperationID:  record.DeletionOperationID,
			IndexID:              record.IndexID,
			IndexName:            record.IndexName,
			TenantID:             record.TenantID,
			ArchivedIndexVersion: record.ArchivedIndexVersion,
			CreatedAtUnixMicro:   record.OperationCreatedAtUnixMicro,
		},
	)
	attempt, attemptErr := indexDeletionMutationAttemptForOperation(
		indexDeletionMutationAttemptRecord{
			DeletionOperationID: record.DeletionOperationID,
			CorrelationID:       record.CorrelationID,
			TenantID:            record.TenantID,
			ClickHouseDatabase:  record.ClickHouseDatabase,
			ClickHouseTable:     record.ClickHouseTable,
			ClickHouseTableUUID: record.ClickHouseTableUUID,
			ProtocolVersion:     record.ProtocolVersion,
			CreatedAtUnixMicro:  record.AttemptCreatedAtUnixMicro,
		},
		operation,
	)
	if operationErr != nil ||
		attemptErr != nil ||
		record.DeletingIndexVersion != record.ArchivedIndexVersion+1 ||
		record.CompletedAtUnixMicro < attempt.CreatedAt.UnixMicro() ||
		record.CompletedAtUnixMicro > maximumControlTimestampUnixMicro {
		return IndexDataDeletionCompletion{},
			invalidIndexDataDeletionCompletion()
	}
	return IndexDataDeletionCompletion{
		DeletionOperationID: operation.ID,
		CorrelationID:       attempt.CorrelationID,
		IndexID:             operation.IndexID,
		IndexName:           operation.IndexName,
		ArchivedVersion:     operation.ArchivedVersion,
		DeletedVersion:      operation.DeletingVersion,
		Target:              attempt.Target,
		ProtocolVersion:     attempt.ProtocolVersion,
		OperationCreatedAt:  operation.CreatedAt,
		MutationCreatedAt:   attempt.CreatedAt,
		CompletedAt:         time.UnixMicro(record.CompletedAtUnixMicro).UTC(),
	}, nil
}

func validateExpectedIndexDeletionMutationAttempt(
	expected IndexDeletionMutationAttempt,
) error {
	canonicalName, nameErr := NormalizeIndexName(expected.IndexName)
	if !protocolid.Valid(expected.DeletionOperationID) ||
		!protocolid.Valid(expected.CorrelationID) ||
		strings.TrimSpace(expected.IndexID) == "" ||
		nameErr != nil ||
		canonicalName != expected.IndexName ||
		validateIndexDeletionMutationTarget(expected.Target) != nil ||
		expected.ProtocolVersion !=
			IndexDeletionMutationProtocolVersion ||
		expected.CreatedAt.IsZero() ||
		!databaseTime(expected.CreatedAt).Equal(expected.CreatedAt) ||
		expected.CreatedAt.UnixMicro() < 1 ||
		expected.CreatedAt.UnixMicro() > maximumControlTimestampUnixMicro {
		return fmt.Errorf(
			"%w: expected index deletion mutation attempt is invalid",
			ErrInvalidArgument,
		)
	}
	return nil
}

func sameIndexDeletionMutationAttempt(
	left IndexDeletionMutationAttempt,
	right IndexDeletionMutationAttempt,
) bool {
	return left.CorrelationID == right.CorrelationID &&
		left.DeletionOperationID == right.DeletionOperationID &&
		left.IndexID == right.IndexID &&
		left.IndexName == right.IndexName &&
		left.Target == right.Target &&
		left.ProtocolVersion == right.ProtocolVersion &&
		left.CreatedAt.Equal(right.CreatedAt)
}

// IndexDataDeletionCompletionMatchesAttempt reports whether a validated
// immutable completion copies the exact durable mutation-attempt identity.
func IndexDataDeletionCompletionMatchesAttempt(
	completion IndexDataDeletionCompletion,
	attempt IndexDeletionMutationAttempt,
) bool {
	return sameIndexDeletionMutationAttempt(
		IndexDeletionMutationAttempt{
			CorrelationID:       completion.CorrelationID,
			DeletionOperationID: completion.DeletionOperationID,
			IndexID:             completion.IndexID,
			IndexName:           completion.IndexName,
			Target:              completion.Target,
			ProtocolVersion:     completion.ProtocolVersion,
			CreatedAt:           completion.MutationCreatedAt,
		},
		attempt,
	)
}

func invalidIndexDataDeletionCompletion() error {
	return errors.New(
		"invalid index data deletion completion in control-plane database",
	)
}
