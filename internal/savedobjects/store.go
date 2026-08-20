package savedobjects

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

const (
	minimumCursorKeyBytes = 32
	maximumIDAttempts     = 4
)

// AccessScope is the authenticated single-user ownership boundary. All
// lookups are owner-scoped so object existence is not disclosed across it.
type AccessScope struct {
	OwnerID string
}

// Options contains process dependencies. CursorKey is required and must be a
// stable secret so issued page tokens survive restarts and cannot be forged.
// Clock and IDGenerator have safe production defaults and are injectable for
// deterministic tests.
type Options struct {
	Clock       func() time.Time
	IDGenerator func() (string, error)
	CursorKey   []byte
}

// ListRequest defines one stable, keyset-paginated saved-search listing.
type ListRequest struct {
	PageSize            uint32
	PageToken           string
	IncludeTotal        bool
	AppIDFilter         *string
	TextFilter          *string
	SharingScopeFilters []opensplunk.SharingScope
	SortBy              opensplunk.SavedSearchSortBy
	SortDirection       opensplunk.SortDirection
}

// ListResult is a detached list page. TotalSize is non-nil only when requested
// and is always exact for the filter used to produce the page.
type ListResult struct {
	SavedSearches  []*opensplunk.SavedSearch
	NextPageToken  *string
	TotalSize      *uint64
	TotalSizeExact bool
}

// Store owns saved-search persistence over an already configured control DB.
type Store struct {
	orm         *gorm.DB
	clock       func() time.Time
	idGenerator func() (string, error)
	cursorKey   []byte
}

// New constructs a saved-search store. The cursor key is intentionally not
// generated here: a random process-local key would invalidate tokens after a
// restart, while a built-in key would make them forgeable.
func New(db *control.DB, options Options) (*Store, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", control.ErrInvalidArgument)
	}
	if len(options.CursorKey) < minimumCursorKeyBytes {
		return nil, fmt.Errorf("%w: cursor key must contain at least %d bytes", control.ErrInvalidArgument, minimumCursorKeyBytes)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = newSavedSearchID
	}
	return &Store{
		orm:         db.GORMDB(),
		clock:       clock,
		idGenerator: idGenerator,
		cursorKey:   slices.Clone(options.CursorKey),
	}, nil
}

// Create persists a normalized definition at version one.
func (store *Store) Create(
	ctx context.Context,
	scope AccessScope,
	definition *opensplunk.SavedSearchDefinition,
) (*opensplunk.SavedSearch, error) {
	return store.create(ctx, scope, definition, nil)
}

func (store *Store) create(
	ctx context.Context,
	scope AccessScope,
	definition *opensplunk.SavedSearchDefinition,
	publisher savedSearchMutationAuditPublisher,
) (*opensplunk.SavedSearch, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	normalized, indexed, encoded, err := normalizeAndEncodeDefinition(definition, ownerID)
	if err != nil {
		return nil, err
	}
	now, err := normalizeClockTime(store.clock())
	if err != nil {
		return nil, err
	}

	for range maximumIDAttempts {
		id, err := store.idGenerator()
		if err != nil {
			return nil, fmt.Errorf("generate saved-search ID: %w", err)
		}
		if err := validateObjectID(id); err != nil {
			return nil, errors.New("generate saved-search ID: generator returned an invalid ID")
		}
		record := newSavedSearchRecord(id, ownerID, indexed, encoded, now)
		var createErr error
		if publisher == nil {
			createErr = store.orm.WithContext(ctx).Create(&record).Error
			if createErr == nil {
				return buildSavedSearch(id, 1, normalized, now, now), nil
			}
		} else {
			tx := store.orm.WithContext(ctx).Begin()
			if tx.Error != nil {
				return nil, mapContextError(ctx, "begin saved-search create", tx.Error)
			}
			createErr = tx.Create(&record).Error
			if createErr == nil {
				if err := publishSavedSearchMutationAudit(
					ctx,
					tx,
					publisher,
					SavedSearchMutationAuditEvent{
						OccurredAt:         now,
						Action:             SavedSearchMutationAuditActionCreate,
						SavedSearchID:      id,
						SavedSearchVersion: 1,
					},
				); err != nil {
					return nil, rollbackSavedSearchTx(tx, err)
				}
				if err := tx.Commit().Error; err != nil {
					return nil, rollbackSavedSearchTx(
						tx,
						mapContextError(ctx, "commit saved-search create", err),
					)
				}
				return buildSavedSearch(id, 1, normalized, now, now), nil
			}
			if err := rollbackSavedSearchTx(tx, nil); err != nil {
				return nil, err
			}
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("create saved search: %w", contextErr)
		}
		conflict, conflictErr := store.nameConflict(ctx, ownerID, indexed.appID, indexed.name, "")
		if conflictErr != nil {
			return nil, conflictErr
		}
		if conflict {
			return nil, fmt.Errorf("%w: saved search %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
		}
		var existing savedSearchRecord
		idErr := store.orm.WithContext(ctx).
			Select("saved_search_id").
			Where("saved_search_id = ?", id).
			Take(&existing).Error
		switch {
		case idErr == nil:
			continue
		case errors.Is(idErr, gorm.ErrRecordNotFound):
			return nil, fmt.Errorf("create saved search: %w", createErr)
		default:
			return nil, fmt.Errorf("check saved-search ID collision: %w", idErr)
		}
	}
	return nil, errors.New("create saved search: repeated random ID collision")
}

// Get returns a detached saved search owned by scope.
func (store *Store) Get(ctx context.Context, scope AccessScope, id string) (*opensplunk.SavedSearch, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	if err := validateObjectID(id); err != nil {
		return nil, err
	}
	record, err := takeSavedSearchRecord(
		store.orm.WithContext(ctx),
		"saved_search_id = ? AND owner_id = ?",
		id,
		ownerID,
	)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("get saved search: %w", contextErr)
		}
		return nil, fmt.Errorf("get saved search: %w", err)
	}
	result, err := savedSearchFromRecord(record)
	if err != nil {
		return nil, fmt.Errorf("get saved search: %w", err)
	}
	return result, nil
}

// Update applies a top-level SavedSearchDefinition field mask under
// optimistic locking. A nil or empty mask replaces the full definition.
func (store *Store) Update(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
	definition *opensplunk.SavedSearchDefinition,
	updateMask *fieldmaskpb.FieldMask,
) (*opensplunk.SavedSearch, error) {
	return store.update(
		ctx,
		scope,
		id,
		expectedVersion,
		definition,
		updateMask,
		nil,
	)
}

func (store *Store) update(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
	definition *opensplunk.SavedSearchDefinition,
	updateMask *fieldmaskpb.FieldMask,
	publisher savedSearchMutationAuditPublisher,
) (result *opensplunk.SavedSearch, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	if err := validateObjectID(id); err != nil {
		return nil, err
	}
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return nil, err
	}
	paths, err := normalizeUpdateMask(updateMask)
	if err != nil {
		return nil, err
	}
	if definition == nil {
		return nil, fmt.Errorf("%w: saved-search definition is required", control.ErrInvalidArgument)
	}

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, mapContextError(ctx, "begin saved-search update", tx.Error)
	}
	defer finishSavedSearchTx(tx, &returnedErr)

	currentRecord, err := takeSavedSearchRecord(
		tx,
		"saved_search_id = ? AND owner_id = ?",
		id,
		ownerID,
	)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read saved search for update: %w", err)
	}
	current, err := savedSearchFromRecord(currentRecord)
	if err != nil {
		return nil, fmt.Errorf("read saved search for update: %w", err)
	}
	if current.Version != expectedVersion {
		return nil, control.ErrVersionConflict
	}
	patched, err := applyDefinitionUpdate(current.Definition, definition, paths)
	if err != nil {
		return nil, err
	}
	normalized, indexed, encoded, err := normalizeAndEncodeDefinition(patched, ownerID)
	if err != nil {
		return nil, err
	}
	conflict, err := nameConflictQuery(tx, ownerID, indexed.appID, indexed.name, id)
	if err != nil {
		return nil, fmt.Errorf("check saved-search name conflict: %w", err)
	}
	if conflict {
		return nil, fmt.Errorf("%w: saved search %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
	}
	now, err := nextUpdateTime(store.clock(), current.UpdatedAt.AsTime())
	if err != nil {
		return nil, err
	}
	// #nosec G115 -- validateExpectedVersion bounds expectedVersion by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	update := tx.Model(&savedSearchRecord{}).
		Where(
			"saved_search_id = ? AND owner_id = ? AND version = ?",
			id,
			ownerID,
			expectedVersionDB,
		).
		Updates(map[string]any{
			"app_id":                indexed.appID,
			"definition_proto":      encoded,
			"name":                  indexed.name,
			"sharing_scope":         int64(indexed.sharingScope),
			"updated_at_unix_micro": now.UnixMicro(),
			"version":               gorm.Expr("version + 1"),
		})
	if update.Error != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("update saved search: %w", contextErr)
		}
		return nil, fmt.Errorf("update saved search: %w", update.Error)
	}
	if update.RowsAffected != 1 {
		return nil, control.ErrVersionConflict
	}
	newVersion := expectedVersion + 1
	if err := publishSavedSearchMutationAudit(
		ctx,
		tx,
		publisher,
		SavedSearchMutationAuditEvent{
			OccurredAt:         now,
			Action:             SavedSearchMutationAuditActionUpdate,
			SavedSearchID:      id,
			SavedSearchVersion: newVersion,
		},
	); err != nil {
		return nil, err
	}
	if err := tx.Commit().Error; err != nil {
		return nil, mapContextError(ctx, "commit saved-search update", err)
	}
	result = buildSavedSearch(
		id,
		newVersion,
		normalized,
		current.CreatedAt.AsTime(),
		now,
	)
	return result, nil
}

// Duplicate atomically clones a source definition into a new stable object.
func (store *Store) Duplicate(
	ctx context.Context,
	scope AccessScope,
	sourceID string,
	newName string,
	destinationAppID *string,
) (*opensplunk.SavedSearch, error) {
	return store.duplicate(
		ctx,
		scope,
		sourceID,
		newName,
		destinationAppID,
		nil,
	)
}

func (store *Store) duplicate(
	ctx context.Context,
	scope AccessScope,
	sourceID string,
	newName string,
	destinationAppID *string,
	publisher savedSearchMutationAuditPublisher,
) (result *opensplunk.SavedSearch, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return nil, err
	}
	if err := validateObjectID(sourceID); err != nil {
		return nil, err
	}

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, mapContextError(ctx, "begin saved-search duplicate", tx.Error)
	}
	defer finishSavedSearchTx(tx, &returnedErr)
	sourceRecord, err := takeSavedSearchRecord(
		tx,
		"saved_search_id = ? AND owner_id = ?",
		sourceID,
		ownerID,
	)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read saved search for duplicate: %w", err)
	}
	source, err := savedSearchFromRecord(sourceRecord)
	if err != nil {
		return nil, fmt.Errorf("read saved search for duplicate: %w", err)
	}
	definition := proto.Clone(source.Definition).(*opensplunk.SavedSearchDefinition)
	definition.Name = newName
	if destinationAppID != nil {
		if definition.Search == nil {
			definition.Search = &opensplunk.SearchDefinition{}
		}
		definition.Search.AppId = stringPointer(*destinationAppID)
	}
	normalized, indexed, encoded, err := normalizeAndEncodeDefinition(definition, ownerID)
	if err != nil {
		return nil, err
	}
	conflict, err := nameConflictQuery(tx, ownerID, indexed.appID, indexed.name, "")
	if err != nil {
		return nil, fmt.Errorf("check duplicate saved-search name: %w", err)
	}
	if conflict {
		return nil, fmt.Errorf("%w: saved search %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
	}
	now, err := normalizeClockTime(store.clock())
	if err != nil {
		return nil, err
	}
	for range maximumIDAttempts {
		id, idErr := store.idGenerator()
		if idErr != nil {
			return nil, fmt.Errorf("generate duplicate saved-search ID: %w", idErr)
		}
		if validationErr := validateObjectID(id); validationErr != nil {
			return nil, errors.New("generate duplicate saved-search ID: generator returned an invalid ID")
		}
		record := newSavedSearchRecord(id, ownerID, indexed, encoded, now)
		create := tx.Create(&record)
		if create.Error == nil {
			if err := publishSavedSearchMutationAudit(
				ctx,
				tx,
				publisher,
				SavedSearchMutationAuditEvent{
					OccurredAt:         now,
					Action:             SavedSearchMutationAuditActionDuplicate,
					SavedSearchID:      id,
					SavedSearchVersion: 1,
				},
			); err != nil {
				return nil, err
			}
			if err := tx.Commit().Error; err != nil {
				return nil, mapContextError(ctx, "commit saved-search duplicate", err)
			}
			result = buildSavedSearch(id, 1, normalized, now, now)
			return result, nil
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("duplicate saved search: %w", contextErr)
		}
		conflict, conflictErr := nameConflictQuery(tx, ownerID, indexed.appID, indexed.name, "")
		if conflictErr != nil {
			return nil, fmt.Errorf("check duplicate saved-search name: %w", conflictErr)
		}
		if conflict {
			return nil, fmt.Errorf("%w: saved search %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
		}
		var existing savedSearchRecord
		idErr = tx.
			Select("saved_search_id").
			Where("saved_search_id = ?", id).
			Take(&existing).Error
		if idErr == nil {
			continue
		}
		if errors.Is(idErr, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("duplicate saved search: %w", create.Error)
		}
		return nil, fmt.Errorf("check duplicate saved-search ID collision: %w", idErr)
	}
	return nil, errors.New("duplicate saved search: repeated random ID collision")
}

// Delete removes an owned saved search under optimistic locking.
func (store *Store) Delete(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
) error {
	return store.delete(ctx, scope, id, expectedVersion, nil)
}

func (store *Store) delete(
	ctx context.Context,
	scope AccessScope,
	id string,
	expectedVersion uint64,
	publisher savedSearchMutationAuditPublisher,
) (returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return err
	}
	ownerID, err := normalizeScope(scope)
	if err != nil {
		return err
	}
	if err := validateObjectID(id); err != nil {
		return err
	}
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return mapContextError(ctx, "begin saved-search delete", tx.Error)
	}
	defer finishSavedSearchTx(tx, &returnedErr)
	var current savedSearchRecord
	err = tx.
		Select("saved_search_id", "version").
		Where("saved_search_id = ? AND owner_id = ?", id, ownerID).
		Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return control.ErrNotFound
	}
	if err != nil {
		return mapContextError(ctx, "read saved search for delete", err)
	}
	// #nosec G115 -- validateExpectedVersion bounds expectedVersion by math.MaxInt64.
	expectedVersionDB := int64(expectedVersion)
	if current.Version != expectedVersionDB {
		return control.ErrVersionConflict
	}
	var occurredAt time.Time
	if publisher != nil {
		occurredAt, err = normalizeClockTime(store.clock())
		if err != nil {
			return err
		}
	}
	deleted := tx.
		Where(
			"saved_search_id = ? AND owner_id = ? AND version = ?",
			id,
			ownerID,
			expectedVersionDB,
		).
		Delete(&savedSearchRecord{})
	if deleted.Error != nil {
		return mapContextError(ctx, "delete saved search", deleted.Error)
	}
	if deleted.RowsAffected != 1 {
		return control.ErrVersionConflict
	}
	if publisher != nil {
		if err := publishSavedSearchMutationAudit(
			ctx,
			tx,
			publisher,
			SavedSearchMutationAuditEvent{
				OccurredAt:         occurredAt,
				Action:             SavedSearchMutationAuditActionDelete,
				SavedSearchID:      id,
				SavedSearchVersion: expectedVersion,
			},
		); err != nil {
			return err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return mapContextError(ctx, "commit saved-search delete", err)
	}
	return nil
}

func savedSearchFromRecord(record savedSearchRecord) (*opensplunk.SavedSearch, error) {
	if validateObjectID(record.SavedSearchID) != nil ||
		record.Version < 1 ||
		record.SharingScope < 1 ||
		record.SharingScope > 3 ||
		record.UpdatedAtUnixMicro < record.CreatedAtUnixMicro {
		return nil, errors.New("invalid saved-search record in control-plane database")
	}
	definition := new(opensplunk.SavedSearchDefinition)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.DefinitionProto, definition); err != nil {
		return nil, fmt.Errorf("decode saved-search definition: %w", err)
	}
	normalized, indexed, _, err := normalizeAndEncodeDefinition(definition, record.OwnerID)
	if err != nil {
		// The definition originated from durable storage, not the active request.
		// Do not preserve ErrInvalidArgument or the HTTP layer could misclassify
		// database corruption as a client-side 400 response.
		return nil, errors.New("invalid saved-search definition in control-plane database")
	}
	if record.Name != indexed.name ||
		record.AppID != indexed.appID ||
		record.OwnerID != indexed.ownerID ||
		record.SharingScope != int64(indexed.sharingScope) {
		return nil, errors.New("saved-search indexed metadata does not match definition in control-plane database")
	}
	created := time.UnixMicro(record.CreatedAtUnixMicro).UTC()
	updated := time.UnixMicro(record.UpdatedAtUnixMicro).UTC()
	if timestamppb.New(created).CheckValid() != nil || timestamppb.New(updated).CheckValid() != nil {
		return nil, errors.New("saved-search record contains a timestamp outside the protobuf range")
	}
	return buildSavedSearch(record.SavedSearchID, uint64(record.Version), normalized, created, updated), nil
}

func buildSavedSearch(id string, version uint64, definition *opensplunk.SavedSearchDefinition, created, updated time.Time) *opensplunk.SavedSearch {
	return &opensplunk.SavedSearch{
		SavedSearchId: strings.Clone(id),
		Version:       version,
		Definition:    proto.Clone(definition).(*opensplunk.SavedSearchDefinition),
		CreatedAt:     timestamppb.New(created),
		UpdatedAt:     timestamppb.New(updated),
	}
}

func newSavedSearchRecord(
	id string,
	ownerID string,
	indexed indexedDefinition,
	encoded []byte,
	now time.Time,
) savedSearchRecord {
	return savedSearchRecord{
		SavedSearchID:      id,
		Version:            1,
		Name:               indexed.name,
		AppID:              indexed.appID,
		OwnerID:            ownerID,
		SharingScope:       int64(indexed.sharingScope),
		DefinitionProto:    slices.Clone(encoded),
		CreatedAtUnixMicro: now.UnixMicro(),
		UpdatedAtUnixMicro: now.UnixMicro(),
	}
}

func takeSavedSearchRecord(database *gorm.DB, query string, args ...any) (savedSearchRecord, error) {
	var record savedSearchRecord
	result := database.Where(query, args...).Take(&record)
	return record, result.Error
}

func (store *Store) nameConflict(ctx context.Context, ownerID, appID, name, excludingID string) (bool, error) {
	conflict, err := nameConflictQuery(
		store.orm.WithContext(ctx),
		ownerID,
		appID,
		name,
		excludingID,
	)
	if err != nil {
		return false, fmt.Errorf("check saved-search name conflict: %w", err)
	}
	return conflict, nil
}

func nameConflictQuery(database *gorm.DB, ownerID, appID, name, excludingID string) (bool, error) {
	var existing savedSearchRecord
	err := database.
		Select("saved_search_id").
		Where(
			"owner_id = ? AND app_id = ? AND name = ? AND saved_search_id <> ?",
			ownerID,
			appID,
			name,
			excludingID,
		).
		Take(&existing).Error
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return false, err
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func mapContextError(ctx context.Context, operation string, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%s: %w", operation, ctx.Err())
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func validateExpectedVersion(version uint64) error {
	if version == 0 || version > math.MaxInt64 {
		return fmt.Errorf("%w: expected version is outside the supported range", control.ErrInvalidArgument)
	}
	return nil
}

func normalizeScope(scope AccessScope) (string, error) {
	ownerID := strings.TrimSpace(scope.OwnerID)
	if err := validateIdentifierText("owner ID", ownerID, 255, false); err != nil {
		return "", err
	}
	return ownerID, nil
}

func validateObjectID(id string) error {
	if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || validateIdentifierText("saved-search ID", id, 128, false) != nil {
		return fmt.Errorf("%w: saved-search ID must contain between 1 and 128 non-whitespace-padded bytes", control.ErrInvalidArgument)
	}
	return nil
}

func normalizeClockTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, errors.New("saved-search clock returned the zero time")
	}
	normalized := time.UnixMicro(value.UTC().UnixMicro()).UTC()
	if err := timestamppb.New(normalized).CheckValid(); err != nil {
		return time.Time{}, errors.New("saved-search clock returned a time outside the protobuf timestamp range")
	}
	return normalized, nil
}

func nextUpdateTime(value time.Time, previous time.Time) (time.Time, error) {
	normalized, err := normalizeClockTime(value)
	if err != nil {
		return time.Time{}, err
	}
	previous = time.UnixMicro(previous.UTC().UnixMicro()).UTC()
	if normalized.After(previous) {
		return normalized, nil
	}
	if previous.UnixMicro() == math.MaxInt64 {
		return time.Time{}, errors.New("update saved search: timestamp space is exhausted")
	}
	advanced, err := normalizeClockTime(time.UnixMicro(previous.UnixMicro() + 1).UTC())
	if err != nil {
		return time.Time{}, errors.New("update saved search: timestamp space is exhausted")
	}
	return advanced, nil
}

func finishSavedSearchTx(tx *gorm.DB, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	*returnedErr = rollbackSavedSearchTx(tx, *returnedErr)
}

func rollbackSavedSearchTx(tx *gorm.DB, cause error) error {
	if tx == nil {
		return cause
	}
	if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) {
		return errors.Join(
			cause,
			fmt.Errorf("roll back saved-search transaction: %w", err),
		)
	}
	return cause
}

func newSavedSearchID() (string, error) {
	buffer := make([]byte, 18)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "ss_" + base64.RawURLEncoding.EncodeToString(buffer), nil
}

func stringPointer(value string) *string {
	value = strings.Clone(value)
	return &value
}
