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

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	SharingScopeFilters []opensplunkv1.SharingScope
	SortBy              opensplunkv1.SavedSearchSortBy
	SortDirection       opensplunkv1.SortDirection
}

// ListResult is a detached list page. TotalSize is non-nil only when requested
// and is always exact for the filter used to produce the page.
type ListResult struct {
	SavedSearches  []*opensplunkv1.SavedSearch
	NextPageToken  *string
	TotalSize      *uint64
	TotalSizeExact bool
}

// Store owns saved-search persistence over an already configured control DB.
type Store struct {
	db          *sql.DB
	clock       func() time.Time
	idGenerator func() (string, error)
	cursorKey   []byte
}

// New constructs a saved-search store. The cursor key is intentionally not
// generated here: a random process-local key would invalidate tokens after a
// restart, while a built-in key would make them forgeable.
func New(db *control.DB, options Options) (*Store, error) {
	if db == nil || db.SQLDB() == nil {
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
		db:          db.SQLDB(),
		clock:       clock,
		idGenerator: idGenerator,
		cursorKey:   slices.Clone(options.CursorKey),
	}, nil
}

// Create persists a normalized definition at version one.
func (store *Store) Create(ctx context.Context, scope AccessScope, definition *opensplunkv1.SavedSearchDefinition) (*opensplunkv1.SavedSearch, error) {
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

	for attempt := 0; attempt < maximumIDAttempts; attempt++ {
		id, err := store.idGenerator()
		if err != nil {
			return nil, fmt.Errorf("generate saved-search ID: %w", err)
		}
		if err := validateObjectID(id); err != nil {
			return nil, errors.New("generate saved-search ID: generator returned an invalid ID")
		}
		_, err = store.db.ExecContext(ctx, `
			INSERT INTO saved_searches (
				saved_search_id, version, name, app_id, owner_id,
				sharing_scope, definition_proto,
				created_at_unix_micro, updated_at_unix_micro
			) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)`,
			id, indexed.name, indexed.appID, ownerID, int64(indexed.sharingScope),
			encoded, now.UnixMicro(), now.UnixMicro(),
		)
		if err == nil {
			return buildSavedSearch(id, 1, normalized, now, now), nil
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
		var existingID string
		idErr := store.db.QueryRowContext(ctx, `SELECT saved_search_id FROM saved_searches WHERE saved_search_id = ?`, id).Scan(&existingID)
		switch {
		case idErr == nil:
			continue
		case errors.Is(idErr, sql.ErrNoRows):
			return nil, fmt.Errorf("create saved search: %w", err)
		default:
			return nil, fmt.Errorf("check saved-search ID collision: %w", idErr)
		}
	}
	return nil, errors.New("create saved search: repeated random ID collision")
}

// Get returns a detached saved search owned by scope.
func (store *Store) Get(ctx context.Context, scope AccessScope, id string) (*opensplunkv1.SavedSearch, error) {
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
	result, err := scanSavedSearch(store.db.QueryRowContext(ctx, savedSearchSelect+` WHERE saved_search_id = ? AND owner_id = ?`, id, ownerID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("get saved search: %w", contextErr)
		}
		return nil, fmt.Errorf("get saved search: %w", err)
	}
	return result, nil
}

// Update applies a top-level SavedSearchDefinition field mask under
// optimistic locking. A nil or empty mask replaces the full definition.
func (store *Store) Update(ctx context.Context, scope AccessScope, id string, expectedVersion uint64, definition *opensplunkv1.SavedSearchDefinition, updateMask *fieldmaskpb.FieldMask) (result *opensplunkv1.SavedSearch, returnedErr error) {
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

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapContextError(ctx, "begin saved-search update", err)
	}
	defer finishTx(tx, &returnedErr)

	current, err := scanSavedSearch(tx.QueryRowContext(ctx, savedSearchSelect+` WHERE saved_search_id = ? AND owner_id = ?`, id, ownerID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, control.ErrNotFound
	}
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
	conflict, err := nameConflictQuery(ctx, tx, ownerID, indexed.appID, indexed.name, id)
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
	updateResult, err := tx.ExecContext(ctx, `
		UPDATE saved_searches SET
			version = version + 1,
			name = ?, app_id = ?, sharing_scope = ?, definition_proto = ?,
			updated_at_unix_micro = ?
		WHERE saved_search_id = ? AND owner_id = ? AND version = ?`,
		indexed.name, indexed.appID, int64(indexed.sharingScope), encoded,
		now.UnixMicro(), id, ownerID, int64(expectedVersion),
	)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("update saved search: %w", contextErr)
		}
		return nil, fmt.Errorf("update saved search: %w", err)
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("read saved-search update count: %w", err)
	}
	if rowsAffected != 1 {
		return nil, control.ErrVersionConflict
	}
	result = buildSavedSearch(id, expectedVersion+1, normalized, current.CreatedAt.AsTime(), now)
	if err := tx.Commit(); err != nil {
		return nil, mapContextError(ctx, "commit saved-search update", err)
	}
	return result, nil
}

// Duplicate atomically clones a source definition into a new stable object.
func (store *Store) Duplicate(ctx context.Context, scope AccessScope, sourceID, newName string, destinationAppID *string) (result *opensplunkv1.SavedSearch, returnedErr error) {
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

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapContextError(ctx, "begin saved-search duplicate", err)
	}
	defer finishTx(tx, &returnedErr)
	source, err := scanSavedSearch(tx.QueryRowContext(ctx, savedSearchSelect+` WHERE saved_search_id = ? AND owner_id = ?`, sourceID, ownerID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read saved search for duplicate: %w", err)
	}
	definition := proto.Clone(source.Definition).(*opensplunkv1.SavedSearchDefinition)
	definition.Name = newName
	if destinationAppID != nil {
		if definition.Search == nil {
			definition.Search = &opensplunkv1.SearchDefinition{}
		}
		definition.Search.AppId = stringPointer(*destinationAppID)
	}
	normalized, indexed, encoded, err := normalizeAndEncodeDefinition(definition, ownerID)
	if err != nil {
		return nil, err
	}
	conflict, err := nameConflictQuery(ctx, tx, ownerID, indexed.appID, indexed.name, "")
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
	for attempt := 0; attempt < maximumIDAttempts; attempt++ {
		id, idErr := store.idGenerator()
		if idErr != nil {
			return nil, fmt.Errorf("generate duplicate saved-search ID: %w", idErr)
		}
		if idErr := validateObjectID(id); idErr != nil {
			return nil, errors.New("generate duplicate saved-search ID: generator returned an invalid ID")
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO saved_searches (
				saved_search_id, version, name, app_id, owner_id,
				sharing_scope, definition_proto,
				created_at_unix_micro, updated_at_unix_micro
			) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)`,
			id, indexed.name, indexed.appID, ownerID, int64(indexed.sharingScope),
			encoded, now.UnixMicro(), now.UnixMicro(),
		)
		if err == nil {
			result = buildSavedSearch(id, 1, normalized, now, now)
			if err := tx.Commit(); err != nil {
				return nil, mapContextError(ctx, "commit saved-search duplicate", err)
			}
			return result, nil
		}
		var existing int
		idErr = tx.QueryRowContext(ctx, `SELECT 1 FROM saved_searches WHERE saved_search_id = ?`, id).Scan(&existing)
		if idErr == nil {
			continue
		}
		if errors.Is(idErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("duplicate saved search: %w", err)
		}
		return nil, fmt.Errorf("check duplicate saved-search ID collision: %w", idErr)
	}
	return nil, errors.New("duplicate saved search: repeated random ID collision")
}

// Delete removes an owned saved search under optimistic locking.
func (store *Store) Delete(ctx context.Context, scope AccessScope, id string, expectedVersion uint64) (returnedErr error) {
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return mapContextError(ctx, "begin saved-search delete", err)
	}
	defer finishTx(tx, &returnedErr)
	var currentVersion int64
	err = tx.QueryRowContext(ctx, `SELECT version FROM saved_searches WHERE saved_search_id = ? AND owner_id = ?`, id, ownerID).Scan(&currentVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return control.ErrNotFound
	}
	if err != nil {
		return mapContextError(ctx, "read saved search for delete", err)
	}
	if currentVersion != int64(expectedVersion) {
		return control.ErrVersionConflict
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM saved_searches WHERE saved_search_id = ? AND owner_id = ? AND version = ?`, id, ownerID, int64(expectedVersion))
	if err != nil {
		return mapContextError(ctx, "delete saved search", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read saved-search delete count: %w", err)
	}
	if count != 1 {
		return control.ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return mapContextError(ctx, "commit saved-search delete", err)
	}
	return nil
}

const savedSearchSelect = `
	SELECT saved_search_id, version, name, app_id, owner_id, sharing_scope,
		definition_proto, created_at_unix_micro, updated_at_unix_micro
	FROM saved_searches`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSavedSearch(scanner rowScanner) (*opensplunkv1.SavedSearch, error) {
	var (
		id, name, appID, ownerID string
		version, sharing         int64
		encoded                  []byte
		createdMicro             int64
		updatedMicro             int64
	)
	if err := scanner.Scan(&id, &version, &name, &appID, &ownerID, &sharing, &encoded, &createdMicro, &updatedMicro); err != nil {
		return nil, err
	}
	if validateObjectID(id) != nil || version < 1 || sharing < 1 || sharing > 3 || updatedMicro < createdMicro {
		return nil, errors.New("invalid saved-search record in control-plane database")
	}
	definition := new(opensplunkv1.SavedSearchDefinition)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, definition); err != nil {
		return nil, fmt.Errorf("decode saved-search definition: %w", err)
	}
	normalized, indexed, _, err := normalizeAndEncodeDefinition(definition, ownerID)
	if err != nil {
		// The definition originated from durable storage, not the active request.
		// Do not preserve ErrInvalidArgument or the HTTP layer could misclassify
		// database corruption as a client-side 400 response.
		return nil, fmt.Errorf("invalid saved-search definition in control-plane database: %v", err)
	}
	if name != indexed.name || appID != indexed.appID || ownerID != indexed.ownerID || sharing != int64(indexed.sharingScope) {
		return nil, errors.New("saved-search indexed metadata does not match definition in control-plane database")
	}
	created := time.UnixMicro(createdMicro).UTC()
	updated := time.UnixMicro(updatedMicro).UTC()
	if timestamppb.New(created).CheckValid() != nil || timestamppb.New(updated).CheckValid() != nil {
		return nil, errors.New("saved-search record contains a timestamp outside the protobuf range")
	}
	return buildSavedSearch(id, uint64(version), normalized, created, updated), nil
}

func buildSavedSearch(id string, version uint64, definition *opensplunkv1.SavedSearchDefinition, created, updated time.Time) *opensplunkv1.SavedSearch {
	return &opensplunkv1.SavedSearch{
		SavedSearchId: strings.Clone(id),
		Version:       version,
		Definition:    proto.Clone(definition).(*opensplunkv1.SavedSearchDefinition),
		CreatedAt:     timestamppb.New(created),
		UpdatedAt:     timestamppb.New(updated),
	}
}

func (store *Store) nameConflict(ctx context.Context, ownerID, appID, name, excludingID string) (bool, error) {
	conflict, err := nameConflictQuery(ctx, store.db, ownerID, appID, name, excludingID)
	if err != nil {
		return false, fmt.Errorf("check saved-search name conflict: %w", err)
	}
	return conflict, nil
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func nameConflictQuery(ctx context.Context, queryer queryRower, ownerID, appID, name, excludingID string) (bool, error) {
	var existingID string
	err := queryer.QueryRowContext(ctx, `
		SELECT saved_search_id FROM saved_searches
		WHERE owner_id = ? AND app_id = ? AND name = ? AND saved_search_id <> ?`, ownerID, appID, name, excludingID).Scan(&existingID)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
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

func finishTx(tx *sql.Tx, returnedErr *error) {
	if tx == nil || returnedErr == nil || *returnedErr == nil {
		return
	}
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*returnedErr = errors.Join(*returnedErr, fmt.Errorf("roll back saved-search transaction: %w", err))
	}
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
