package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/protocolid"
	"gorm.io/gorm"
)

var (
	errIndexDeletionOperationIDCollision = errors.New(
		"index deletion operation ID collision",
	)
	errInvalidIndexDeletionOperation = errors.New(
		"invalid index deletion operation in control-plane database",
	)
)

// IndexDeletionOperation is the durable control-plane intent for one physical
// index deletion. Every row is outstanding coordinator work; a future terminal
// transaction will replace it with the appropriate audit marker. TenantID is
// the immutable trusted scope captured when the request is admitted.
// ClickHouse-specific progress is deliberately not represented by this
// control-plane admission record.
type IndexDeletionOperation struct {
	ID              string
	TenantID        string
	IndexID         string
	IndexName       string
	ArchivedVersion uint64
	DeletingVersion uint64
	CreatedAt       time.Time
}

// IndexDataDeletionScope is the trusted tenant boundary captured by physical
// deletion admission. It must come from authenticated process state, never
// from request payload data.
type IndexDataDeletionScope struct {
	TenantID string
}

// BeginIndexDataDeletion atomically records an exact archived index generation
// and transitions that index to the coordinator-owned deleting state. An exact
// retry returns the original operation, including after process restart.
func (db *DB) BeginIndexDataDeletion(
	ctx context.Context,
	scope IndexDataDeletionScope,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
) (IndexDeletionOperation, error) {
	return db.beginIndexDataDeletionWithAudit(
		ctx,
		scope,
		indexID,
		expectedVersion,
		confirmationName,
		nil,
	)
}

func (db *DB) beginIndexDataDeletionWithAudit(
	ctx context.Context,
	scope IndexDataDeletionScope,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
	auditPublisher indexMutationAuditPublisher,
) (IndexDeletionOperation, error) {
	if validateTenantID(scope.TenantID) != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: index data deletion tenant ID is invalid",
			ErrInvalidArgument,
		)
	}
	if err := validateIndexDataDeletionVersion(expectedVersion); err != nil {
		return IndexDeletionOperation{}, err
	}
	if strings.TrimSpace(indexID) == "" {
		return IndexDeletionOperation{}, fmt.Errorf("%w: index ID is required", ErrInvalidArgument)
	}
	canonicalConfirmation, err := NormalizeIndexName(confirmationName)
	if err != nil || canonicalConfirmation != confirmationName {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: index deletion confirmation is invalid",
			ErrInvalidArgument,
		)
	}
	existing, found, err := db.findIndexDeletionOperation(
		ctx,
		scope.TenantID,
		indexID,
		expectedVersion,
		canonicalConfirmation,
	)
	if err != nil || found {
		return existing, err
	}

	for range 3 {
		operationID, generateErr := randomID("idxdel_")
		if generateErr != nil {
			return IndexDeletionOperation{}, fmt.Errorf(
				"generate index deletion operation ID: %w",
				generateErr,
			)
		}
		operation, beginErr := db.beginIndexDataDeletionTransaction(
			ctx,
			operationID,
			scope.TenantID,
			indexID,
			expectedVersion,
			canonicalConfirmation,
			auditPublisher,
		)
		if !errors.Is(beginErr, errIndexDeletionOperationIDCollision) {
			return operation, beginErr
		}
	}
	return IndexDeletionOperation{}, errors.New(
		"begin index data deletion: repeated random operation ID collision",
	)
}

func (db *DB) findIndexDeletionOperation(
	ctx context.Context,
	tenantID string,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
) (IndexDeletionOperation, bool, error) {
	record, found, err := takeIndexDeletionOperationByIndex(
		db.orm.WithContext(ctx),
		indexID,
	)
	if err != nil {
		return IndexDeletionOperation{}, false, fmt.Errorf(
			"read existing index deletion operation: %w",
			err,
		)
	}
	if !found {
		return IndexDeletionOperation{}, false, nil
	}
	operation, err := matchingIndexDeletionOperation(
		db.orm.WithContext(ctx),
		record,
		tenantID,
		expectedVersion,
		confirmationName,
	)
	if err != nil {
		return IndexDeletionOperation{}, false, fmt.Errorf(
			"read existing index deletion operation: %w",
			err,
		)
	}
	return operation, true, nil
}

func (db *DB) beginIndexDataDeletionTransaction(
	ctx context.Context,
	operationID string,
	tenantID string,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
	auditPublisher indexMutationAuditPublisher,
) (result IndexDeletionOperation, err error) {
	tx := db.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"begin index data deletion transaction: %w",
			tx.Error,
		)
	}
	defer finishGORMTx(tx, &err)

	existing, found, readExistingErr := takeIndexDeletionOperationByIndex(tx, indexID)
	if readExistingErr != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"read existing index deletion operation: %w",
			readExistingErr,
		)
	}
	if found {
		operation, conversionErr := matchingIndexDeletionOperation(
			tx,
			existing,
			tenantID,
			expectedVersion,
			confirmationName,
		)
		if conversionErr != nil {
			return IndexDeletionOperation{}, fmt.Errorf(
				"read existing index deletion operation: %w",
				conversionErr,
			)
		}
		if commitErr := tx.Commit().Error; commitErr != nil {
			return IndexDeletionOperation{}, fmt.Errorf(
				"commit idempotent index data deletion: %w",
				commitErr,
			)
		}
		return operation, nil
	}

	currentRecord, readErr := takeIndexRecord(tx, "index_id = ?", indexID)
	if errors.Is(readErr, gorm.ErrRecordNotFound) {
		return IndexDeletionOperation{}, ErrNotFound
	}
	if readErr != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"read index for data deletion: %w",
			readErr,
		)
	}
	current, conversionErr := indexFromRecord(currentRecord)
	if conversionErr != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"read index for data deletion: %w",
			conversionErr,
		)
	}
	if current.Version != expectedVersion {
		return IndexDeletionOperation{}, ErrVersionConflict
	}
	if current.State != IndexStateArchived {
		return IndexDeletionOperation{}, ErrDependencyConflict
	}
	if current.Definition.Name != confirmationName {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: confirmation name must exactly match the canonical index name",
			ErrInvalidArgument,
		)
	}
	createdAt := databaseTime(time.Now())
	if current.UpdatedAt.After(createdAt) {
		createdAt = current.UpdatedAt
	}
	record := indexDeletionOperationRecord{
		DeletionOperationID:  operationID,
		IndexID:              current.ID,
		IndexName:            current.Definition.Name,
		TenantID:             tenantID,
		ArchivedIndexVersion: currentRecord.Version,
		CreatedAtUnixMicro:   createdAt.UnixMicro(),
	}
	create := tx.Create(&record)
	if create.Error != nil {
		collision, collisionErr := indexDeletionOperationIDInUse(tx, operationID)
		if collisionErr != nil {
			return IndexDeletionOperation{}, errors.Join(
				fmt.Errorf("create index deletion operation: %w", create.Error),
				fmt.Errorf(
					"check index deletion operation ID collision: %w",
					collisionErr,
				),
			)
		}
		if collision {
			return IndexDeletionOperation{}, errIndexDeletionOperationIDCollision
		}
		return IndexDeletionOperation{}, fmt.Errorf(
			"create index deletion operation: %w",
			create.Error,
		)
	}
	operation, conversionErr := validatedIndexDeletionOperation(tx, record)
	if conversionErr != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"read created index deletion operation: %w",
			conversionErr,
		)
	}
	if err := publishIndexMutationAudit(
		ctx,
		tx,
		auditPublisher,
		IndexMutationAuditEvent{
			OccurredAt:   operation.CreatedAt,
			Action:       IndexMutationAuditActionDeleteData,
			IndexID:      operation.IndexID,
			IndexVersion: operation.DeletingVersion,
		},
	); err != nil {
		return IndexDeletionOperation{}, err
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"commit index data deletion: %w",
			commitErr,
		)
	}
	return operation, nil
}

func indexDeletionOperationIDInUse(
	database *gorm.DB,
	operationID string,
) (bool, error) {
	return queryIndexDeletionIdentityInUse(
		database,
		`
			SELECT EXISTS (
				SELECT 1
				FROM index_deletion_operations
				WHERE deletion_operation_id = ?
				UNION ALL
				SELECT 1
				FROM index_data_deletion_completions
				WHERE deletion_operation_id = ?
			)`,
		operationID,
		operationID,
	)
}

func takeIndexDeletionOperationByIndex(
	database *gorm.DB,
	indexID string,
) (indexDeletionOperationRecord, bool, error) {
	var record indexDeletionOperationRecord
	result := database.Where("index_id = ?", indexID).Take(&record)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return indexDeletionOperationRecord{}, false, nil
	}
	if result.Error != nil {
		return indexDeletionOperationRecord{}, false, result.Error
	}
	return record, true, nil
}

func matchingIndexDeletionOperation(
	database *gorm.DB,
	record indexDeletionOperationRecord,
	tenantID string,
	expectedVersion uint64,
	confirmationName string,
) (IndexDeletionOperation, error) {
	operation, err := validatedIndexDeletionOperation(database, record)
	if err != nil {
		return IndexDeletionOperation{}, err
	}
	if operation.TenantID != tenantID {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: index data deletion tenant changed",
			ErrDependencyConflict,
		)
	}
	if operation.ArchivedVersion != expectedVersion {
		return IndexDeletionOperation{}, ErrVersionConflict
	}
	if operation.IndexName != confirmationName {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: confirmation name must exactly match the canonical index name",
			ErrInvalidArgument,
		)
	}
	return operation, nil
}

// GetIndexDeletionOperation returns one durable operation by its stable ID.
func (db *DB) GetIndexDeletionOperation(
	ctx context.Context,
	operationID string,
) (IndexDeletionOperation, error) {
	if !protocolid.Valid(operationID) {
		return IndexDeletionOperation{}, fmt.Errorf(
			"%w: index deletion operation ID is invalid",
			ErrInvalidArgument,
		)
	}
	var record indexDeletionOperationRecord
	result := db.orm.WithContext(ctx).
		Where("deletion_operation_id = ?", operationID).
		Take(&record)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return IndexDeletionOperation{}, ErrNotFound
	}
	if result.Error != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"get index deletion operation: %w",
			result.Error,
		)
	}
	operation, err := validatedIndexDeletionOperation(db.orm.WithContext(ctx), record)
	if err != nil {
		return IndexDeletionOperation{}, fmt.Errorf(
			"get index deletion operation: %w",
			err,
		)
	}
	return operation, nil
}

// NextIndexDeletionOperation returns the oldest outstanding physical-deletion
// intent. Returning a single row keeps restart discovery bounded; a serialized
// coordinator processes that row before asking for the next one.
func (db *DB) NextIndexDeletionOperation(
	ctx context.Context,
) (IndexDeletionOperation, error) {
	database := db.orm.WithContext(ctx)
	for {
		var record indexDeletionOperationRecord
		result := database.
			Order("created_at_unix_micro").
			Order("deletion_operation_id").
			Take(&record)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return IndexDeletionOperation{}, ErrNotFound
		}
		if result.Error != nil {
			return IndexDeletionOperation{}, fmt.Errorf(
				"get next index deletion operation: %w",
				result.Error,
			)
		}
		operation, err := validatedIndexDeletionOperation(database, record)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return IndexDeletionOperation{}, fmt.Errorf(
				"get next index deletion operation: %w",
				err,
			)
		}
		return operation, nil
	}
}

func validatedIndexDeletionOperation(
	database *gorm.DB,
	record indexDeletionOperationRecord,
) (IndexDeletionOperation, error) {
	operation, err := indexDeletionOperationFromRecord(record)
	if err != nil {
		return IndexDeletionOperation{}, err
	}
	var currentRecord indexRecord
	result := visibleIndexRecords(database).
		Where("index_id = ?", record.IndexID).
		Take(&currentRecord)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return IndexDeletionOperation{}, result.Error
		}
		return IndexDeletionOperation{}, indexDeletionRelationshipError(
			database,
			operation.ID,
			errInvalidIndexDeletionOperation,
		)
	}
	current, err := indexFromRecord(currentRecord)
	if err != nil ||
		current.ID != operation.IndexID ||
		current.Definition.Name != operation.IndexName ||
		current.Version != operation.DeletingVersion ||
		current.State != IndexStateDeleting ||
		currentRecord.UpdatedAtUnixMicro != record.CreatedAtUnixMicro {
		return IndexDeletionOperation{}, indexDeletionRelationshipError(
			database,
			operation.ID,
			errInvalidIndexDeletionOperation,
		)
	}
	return operation, nil
}

func indexDeletionOperationFromRecord(
	record indexDeletionOperationRecord,
) (IndexDeletionOperation, error) {
	canonicalName, nameErr := NormalizeIndexName(record.IndexName)
	if !protocolid.Valid(record.DeletionOperationID) ||
		validateTenantID(record.TenantID) != nil ||
		strings.TrimSpace(record.IndexID) == "" ||
		nameErr != nil ||
		canonicalName != record.IndexName ||
		record.ArchivedIndexVersion < 1 ||
		record.ArchivedIndexVersion == math.MaxInt64 ||
		record.CreatedAtUnixMicro <= 0 {
		return IndexDeletionOperation{}, errInvalidIndexDeletionOperation
	}
	return IndexDeletionOperation{
		ID:              record.DeletionOperationID,
		TenantID:        record.TenantID,
		IndexID:         record.IndexID,
		IndexName:       record.IndexName,
		ArchivedVersion: uint64(record.ArchivedIndexVersion),
		DeletingVersion: uint64(record.ArchivedIndexVersion + 1),
		CreatedAt:       time.UnixMicro(record.CreatedAtUnixMicro).UTC(),
	}, nil
}

// indexDeletionRelationshipError explains a broken deletion relationship: a
// completed deletion is reported as not-found, while anything else is the
// caller's invalid-record sentinel.
func indexDeletionRelationshipError(
	database *gorm.DB,
	operationID string,
	fallback error,
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
	return fallback
}

func validateIndexDataDeletionVersion(version uint64) error {
	if err := validateExpectedVersion(version); err != nil {
		return err
	}
	if version == math.MaxInt64 {
		return fmt.Errorf(
			"%w: expected version cannot enter the deleting generation",
			ErrInvalidArgument,
		)
	}
	return nil
}
