package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// Get returns the current object when version is nil. Historical reads are
// authorized exclusively from the current registry identity. If that identity
// is currently quarantined, every request returns its bodyless current scalar
// projection and never reads a current or historical definition blob.
func (store *Store) Get(
	ctx context.Context,
	scope ReadScope,
	knowledgeObjectID string,
	version *uint64,
) (result Object, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return Object{}, err
	}
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return Object{}, err
	}
	if !validIdentity(knowledgeObjectID, maximumObjectIDBytes) {
		return Object{}, fmt.Errorf("%w: knowledge object identity is invalid", control.ErrInvalidArgument)
	}
	var requestedVersion int64
	if version != nil {
		if *version == 0 || *version > math.MaxInt64 {
			return Object{}, fmt.Errorf("%w: knowledge object version is invalid", control.ErrInvalidArgument)
		}
		requestedVersion = int64(*version)
	}

	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return Object{}, mapError(ctx.Err(), "begin get", tx.Error)
	}
	defer finishReadTransaction(tx, &returnedErr)
	catalog, err := readCatalogState(tx, normalizedScope.tenantID)
	if err != nil {
		return Object{}, mapError(ctx.Err(), "read get revision", err)
	}
	query := tx.Model(&registryRecord{}).Where("knowledge_object_id = ?", knowledgeObjectID)
	query = applyPointGetAuthorization(query, normalizedScope)
	registry, found, err := readRegistryRecord(query)
	if err != nil {
		return Object{}, mapError(ctx.Err(), "read current registry", err)
	}
	if !found {
		return Object{}, control.ErrNotFound
	}
	if !catalog.found || catalog.revision == 0 {
		return Object{}, fmt.Errorf("%w: visible knowledge catalog has no revision authority", ErrCorrupt)
	}
	if err := validateRegistry(registry); err != nil {
		return Object{}, mapError(ctx.Err(), "validate current registry", err)
	}
	current, err := store.objectFromCurrentRegistry(tx, registry)
	if err != nil {
		return Object{}, mapError(ctx.Err(), "validate current object", err)
	}
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if registry.State == StateQuarantined {
		result = current
	} else if version != nil && requestedVersion > registry.CurrentVersion {
		return Object{}, control.ErrNotFound
	} else if version == nil || requestedVersion == registry.CurrentVersion {
		result = current
	} else {
		historical, found, readErr := readVersionRecord(
			tx,
			normalizedScope.tenantID,
			knowledgeObjectID,
			requestedVersion,
		)
		if readErr != nil {
			return Object{}, mapError(ctx.Err(), "read historical version", readErr)
		}
		if !found {
			return Object{}, fmt.Errorf("%w: historical immutable version is missing", ErrCorrupt)
		}
		result, err = store.objectFromHistoricalVersion(tx, registry, historical)
		if err != nil {
			return Object{}, mapError(ctx.Err(), "validate historical version", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	finalCatalog, err := readCatalogState(tx, normalizedScope.tenantID)
	if err != nil {
		return Object{}, mapError(ctx.Err(), "re-read get revision", err)
	}
	if !finalCatalog.equal(catalog) {
		return Object{}, fmt.Errorf("%w: catalog revision changed inside one read snapshot", ErrCorrupt)
	}
	if err := tx.Commit().Error; err != nil {
		return Object{}, mapError(ctx.Err(), "commit get", err)
	}
	return result, nil
}

func finishReadTransaction(tx *gorm.DB, returnedErr *error) {
	if tx == nil || returnedErr == nil || tx.Error != nil {
		return
	}
	if rollbackErr := tx.Rollback().Error; rollbackErr != nil &&
		!errors.Is(rollbackErr, gorm.ErrInvalidTransaction) &&
		!errors.Is(rollbackErr, sql.ErrTxDone) {
		if *returnedErr == nil {
			*returnedErr = fmt.Errorf("rollback knowledge catalog read: %w", rollbackErr)
		} else {
			*returnedErr = errors.Join(*returnedErr, rollbackErr)
		}
	}
}
