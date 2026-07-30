package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	// IndexDeletionMutationProtocolVersion identifies the command-marker
	// grammar understood by the native ClickHouse reconciliation path.
	IndexDeletionMutationProtocolVersion uint32 = 1

	maximumClickHouseIdentifierBytes    = 255
	indexDeletionMutationCorrelationTag = "idxmut_"
)

var errIndexDeletionMutationCorrelationCollision = errors.New(
	"index deletion mutation correlation ID collision",
)

var errInvalidIndexDeletionMutationAttempt = errors.New(
	"invalid index deletion mutation attempt in control-plane database",
)

// IndexDeletionMutationTarget binds a durable deletion attempt to the exact
// admission tenant and physical ClickHouse table generation resolved before
// submission.
type IndexDeletionMutationTarget struct {
	TenantID  string
	Database  string
	Table     string
	TableUUID string
}

// IndexDeletionMutationAttempt is the immutable control-plane record that
// makes an outcome-ambiguous native ALTER restartable. ClickHouse mutation IDs
// are intentionally absent: one stable correlation can discover multiple
// idempotent submissions in system.mutations.
type IndexDeletionMutationAttempt struct {
	CorrelationID       string
	DeletionOperationID string
	IndexID             string
	IndexName           string
	Target              IndexDeletionMutationTarget
	ProtocolVersion     uint32
	CreatedAt           time.Time
}

// EnsureIndexDeletionMutationAttempt creates the one correlation record for
// an outstanding deletion operation. Exact concurrent and restart retries
// return the same row; any tenant or physical-target drift fails closed.
func (db *DB) EnsureIndexDeletionMutationAttempt(
	ctx context.Context,
	deletionOperationID string,
	target IndexDeletionMutationTarget,
) (IndexDeletionMutationAttempt, error) {
	if !protocolid.Valid(deletionOperationID) {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"%w: index deletion operation ID is invalid",
			ErrInvalidArgument,
		)
	}
	if err := validateIndexDeletionMutationTarget(target); err != nil {
		return IndexDeletionMutationAttempt{}, err
	}

	existing, found, err := takeIndexDeletionMutationAttempt(
		db.orm.WithContext(ctx),
		deletionOperationID,
	)
	if err != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read existing index deletion mutation attempt: %w",
			err,
		)
	}
	if found {
		return matchingIndexDeletionMutationAttempt(
			db.orm.WithContext(ctx),
			existing,
			target,
		)
	}

	for range 3 {
		correlationID, generateErr := randomID(
			indexDeletionMutationCorrelationTag,
			16,
		)
		if generateErr != nil {
			return IndexDeletionMutationAttempt{}, fmt.Errorf(
				"generate index deletion mutation correlation ID: %w",
				generateErr,
			)
		}
		attempt, ensureErr := db.ensureIndexDeletionMutationAttempt(
			ctx,
			deletionOperationID,
			correlationID,
			target,
		)
		if !errors.Is(
			ensureErr,
			errIndexDeletionMutationCorrelationCollision,
		) {
			return attempt, ensureErr
		}
	}
	return IndexDeletionMutationAttempt{}, errors.New(
		"ensure index deletion mutation attempt: repeated random correlation ID collision",
	)
}

func (db *DB) ensureIndexDeletionMutationAttempt(
	ctx context.Context,
	deletionOperationID string,
	correlationID string,
	target IndexDeletionMutationTarget,
) (result IndexDeletionMutationAttempt, err error) {
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"begin index deletion mutation attempt transaction: %w",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &err)

	existing, found, readErr := takeIndexDeletionMutationAttempt(
		tx,
		deletionOperationID,
	)
	if readErr != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read existing index deletion mutation attempt: %w",
			readErr,
		)
	}
	if found {
		attempt, conversionErr := matchingIndexDeletionMutationAttempt(
			tx,
			existing,
			target,
		)
		if conversionErr != nil {
			return IndexDeletionMutationAttempt{}, conversionErr
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			return IndexDeletionMutationAttempt{}, fmt.Errorf(
				"commit idempotent index deletion mutation attempt: %w",
				commitErr,
			)
		}
		return attempt, nil
	}

	var operationRecord indexDeletionOperationRecord
	operationResult := tx.
		Where("deletion_operation_id = ?", deletionOperationID).
		Take(&operationRecord)
	if errors.Is(operationResult.Error, gorm.ErrRecordNotFound) {
		return IndexDeletionMutationAttempt{}, ErrNotFound
	}
	if operationResult.Error != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read index deletion operation for mutation attempt: %w",
			operationResult.Error,
		)
	}
	operation, conversionErr := validatedIndexDeletionOperation(
		tx,
		operationRecord,
	)
	if conversionErr != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read index deletion operation for mutation attempt: %w",
			conversionErr,
		)
	}
	if target.TenantID != operation.TenantID {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"%w: index deletion mutation tenant changed",
			ErrDependencyConflict,
		)
	}
	createdAt := databaseTime(time.Now())
	if operation.CreatedAt.After(createdAt) {
		createdAt = operation.CreatedAt
	}
	record := indexDeletionMutationAttemptRecord{
		DeletionOperationID: deletionOperationID,
		CorrelationID:       correlationID,
		TenantID:            target.TenantID,
		ClickHouseDatabase:  target.Database,
		ClickHouseTable:     target.Table,
		ClickHouseTableUUID: target.TableUUID,
		ProtocolVersion:     int64(IndexDeletionMutationProtocolVersion),
		CreatedAtUnixMicro:  createdAt.UnixMicro(),
	}
	create := tx.Create(&record)
	if create.Error != nil {
		collision, collisionErr := indexDeletionMutationCorrelationIDInUse(
			tx,
			correlationID,
		)
		if collisionErr != nil {
			return IndexDeletionMutationAttempt{}, errors.Join(
				fmt.Errorf(
					"create index deletion mutation attempt: %w",
					create.Error,
				),
				fmt.Errorf(
					"check mutation correlation ID collision: %w",
					collisionErr,
				),
			)
		}
		if collision {
			return IndexDeletionMutationAttempt{},
				errIndexDeletionMutationCorrelationCollision
		}
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"create index deletion mutation attempt: %w",
			create.Error,
		)
	}

	attempt, conversionErr := indexDeletionMutationAttemptForOperation(
		record,
		operation,
	)
	if conversionErr != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read created index deletion mutation attempt: %w",
			conversionErr,
		)
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"commit index deletion mutation attempt: %w",
			commitErr,
		)
	}
	return attempt, nil
}

func indexDeletionMutationCorrelationIDInUse(
	database *gorm.DB,
	correlationID string,
) (bool, error) {
	return queryIndexDeletionIdentityInUse(
		database,
		`
			SELECT EXISTS (
				SELECT 1
				FROM index_deletion_mutation_attempts
				WHERE correlation_id = ?
				UNION ALL
				SELECT 1
				FROM index_data_deletion_completions
				WHERE correlation_id = ?
			)`,
		correlationID,
		correlationID,
	)
}

// GetIndexDeletionMutationAttempt returns the immutable attempt for one
// outstanding deletion operation.
func (db *DB) GetIndexDeletionMutationAttempt(
	ctx context.Context,
	deletionOperationID string,
) (IndexDeletionMutationAttempt, error) {
	if !protocolid.Valid(deletionOperationID) {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"%w: index deletion operation ID is invalid",
			ErrInvalidArgument,
		)
	}
	record, found, err := takeIndexDeletionMutationAttempt(
		db.orm.WithContext(ctx),
		deletionOperationID,
	)
	if err != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"get index deletion mutation attempt: %w",
			err,
		)
	}
	if !found {
		return IndexDeletionMutationAttempt{}, ErrNotFound
	}
	attempt, err := validatedIndexDeletionMutationAttempt(
		db.orm.WithContext(ctx),
		record,
	)
	if err != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"get index deletion mutation attempt: %w",
			err,
		)
	}
	return attempt, nil
}

func takeIndexDeletionMutationAttempt(
	database *gorm.DB,
	deletionOperationID string,
) (indexDeletionMutationAttemptRecord, bool, error) {
	var record indexDeletionMutationAttemptRecord
	result := database.
		Where("deletion_operation_id = ?", deletionOperationID).
		Take(&record)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return indexDeletionMutationAttemptRecord{}, false, nil
	}
	if result.Error != nil {
		return indexDeletionMutationAttemptRecord{}, false, result.Error
	}
	return record, true, nil
}

func matchingIndexDeletionMutationAttempt(
	database *gorm.DB,
	record indexDeletionMutationAttemptRecord,
	target IndexDeletionMutationTarget,
) (IndexDeletionMutationAttempt, error) {
	attempt, err := validatedIndexDeletionMutationAttempt(database, record)
	if err != nil {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"read existing index deletion mutation attempt: %w",
			err,
		)
	}
	if attempt.Target != target {
		return IndexDeletionMutationAttempt{}, fmt.Errorf(
			"%w: index deletion mutation target changed",
			ErrDependencyConflict,
		)
	}
	return attempt, nil
}

func validatedIndexDeletionMutationAttempt(
	database *gorm.DB,
	record indexDeletionMutationAttemptRecord,
) (IndexDeletionMutationAttempt, error) {
	attempt, _, err := validatedIndexDeletionMutationAttemptWithOperation(
		database,
		record,
	)
	return attempt, err
}

func validatedIndexDeletionMutationAttemptWithOperation(
	database *gorm.DB,
	record indexDeletionMutationAttemptRecord,
) (
	IndexDeletionMutationAttempt,
	IndexDeletionOperation,
	error,
) {
	var operationRecord indexDeletionOperationRecord
	result := database.
		Where(
			"deletion_operation_id = ?",
			record.DeletionOperationID,
		).
		Take(&operationRecord)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return IndexDeletionMutationAttempt{},
				IndexDeletionOperation{},
				result.Error
		}
		return IndexDeletionMutationAttempt{},
			IndexDeletionOperation{},
			indexDeletionMutationAttemptRelationshipError(
				database,
				record.DeletionOperationID,
			)
	}
	operation, err := validatedIndexDeletionOperation(database, operationRecord)
	if err != nil {
		if !errors.Is(err, errInvalidIndexDeletionOperation) {
			return IndexDeletionMutationAttempt{},
				IndexDeletionOperation{},
				err
		}
		return IndexDeletionMutationAttempt{},
			IndexDeletionOperation{},
			invalidIndexDeletionMutationAttempt()
	}
	attempt, err := indexDeletionMutationAttemptForOperation(record, operation)
	if err != nil {
		return IndexDeletionMutationAttempt{},
			IndexDeletionOperation{},
			err
	}
	return attempt, operation, nil
}

func indexDeletionMutationAttemptForOperation(
	record indexDeletionMutationAttemptRecord,
	operation IndexDeletionOperation,
) (IndexDeletionMutationAttempt, error) {
	attempt, err := indexDeletionMutationAttemptFromRecord(record)
	if err != nil ||
		attempt.DeletionOperationID != operation.ID ||
		attempt.Target.TenantID != operation.TenantID ||
		attempt.CreatedAt.Before(operation.CreatedAt) {
		return IndexDeletionMutationAttempt{},
			invalidIndexDeletionMutationAttempt()
	}
	attempt.IndexID = operation.IndexID
	attempt.IndexName = operation.IndexName
	return attempt, nil
}

func indexDeletionMutationAttemptFromRecord(
	record indexDeletionMutationAttemptRecord,
) (IndexDeletionMutationAttempt, error) {
	target := IndexDeletionMutationTarget{
		TenantID:  record.TenantID,
		Database:  record.ClickHouseDatabase,
		Table:     record.ClickHouseTable,
		TableUUID: record.ClickHouseTableUUID,
	}
	if !protocolid.Valid(record.DeletionOperationID) ||
		!protocolid.Valid(record.CorrelationID) ||
		validateIndexDeletionMutationTarget(target) != nil ||
		record.ProtocolVersion !=
			int64(IndexDeletionMutationProtocolVersion) ||
		record.CreatedAtUnixMicro < 1 ||
		record.CreatedAtUnixMicro > maximumControlTimestampUnixMicro {
		return IndexDeletionMutationAttempt{},
			invalidIndexDeletionMutationAttempt()
	}
	return IndexDeletionMutationAttempt{
		CorrelationID:       record.CorrelationID,
		DeletionOperationID: record.DeletionOperationID,
		Target:              target,
		ProtocolVersion:     uint32(record.ProtocolVersion),
		CreatedAt:           time.UnixMicro(record.CreatedAtUnixMicro).UTC(),
	}, nil
}

func validateIndexDeletionMutationTarget(
	target IndexDeletionMutationTarget,
) error {
	if validateTenantID(target.TenantID) != nil {
		return fmt.Errorf(
			"%w: index deletion mutation tenant ID is invalid",
			ErrInvalidArgument,
		)
	}
	if !validClickHousePhysicalIdentifier(target.Database) {
		return fmt.Errorf(
			"%w: index deletion mutation database is invalid",
			ErrInvalidArgument,
		)
	}
	if !validClickHousePhysicalIdentifier(target.Table) {
		return fmt.Errorf(
			"%w: index deletion mutation table is invalid",
			ErrInvalidArgument,
		)
	}
	parsedUUID, err := uuid.Parse(target.TableUUID)
	if err != nil ||
		parsedUUID == uuid.Nil ||
		parsedUUID.String() != target.TableUUID {
		return fmt.Errorf(
			"%w: index deletion mutation table UUID is invalid",
			ErrInvalidArgument,
		)
	}
	return nil
}

func validClickHousePhysicalIdentifier(value string) bool {
	if len(value) < 1 || len(value) > maximumClickHouseIdentifierBytes {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if index == 0 {
			if character != '_' &&
				(character < 'A' || character > 'Z') &&
				(character < 'a' || character > 'z') {
				return false
			}
			continue
		}
		if character != '_' &&
			(character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func invalidIndexDeletionMutationAttempt() error {
	return errInvalidIndexDeletionMutationAttempt
}

func indexDeletionMutationAttemptRelationshipError(
	database *gorm.DB,
	operationID string,
) error {
	_, completed, err := takeIndexDataDeletionCompletion(
		database,
		operationID,
	)
	if err != nil {
		return err
	}
	if completed {
		return ErrNotFound
	}
	return errInvalidIndexDeletionMutationAttempt
}
