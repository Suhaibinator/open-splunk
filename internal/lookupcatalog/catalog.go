// Package lookupcatalog owns logical lookup definitions and resolves them to
// exact immutable CSV asset versions. Physical uploads remain in lookupasset.
package lookupcatalog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaximumResolvedLookups = 16
	// Matches the external-table execution contract. Resolution rejects the
	// complete metadata batch before loading a single immutable CSV body.
	MaximumResolvedCells = 6_400_000
	MaximumResolvedBytes = MaximumResolvedLookups * lookupasset.MaximumSourceBytes
	MaximumDefinitions   = 2_048
	MaximumVersions      = 8_192
	// Every retained identity can require two terminal versions (disable then
	// delete). Ordinary publication stops below the hard ceiling so those
	// lifecycle operations cannot be stranded by metadata replacements.
	MaximumOrdinaryVersions = MaximumVersions - 2*MaximumDefinitions
	// MaximumManagedLookups is the hard bound for one owner's complete
	// management projection. The service requests one additional row to prove
	// that filtered and sorted pagination never silently truncates the catalog.
	MaximumManagedLookups = 2_048
	MaximumListTextBytes  = 255
	maximumIDAttempts     = 8
	maximumUnixMicro      = int64(253_402_300_799_999_999)
	mutationCreate        = "CREATE"
	mutationReplace       = "REPLACE"
	mutationEnable        = "ENABLE"
	mutationDisable       = "DISABLE"
	mutationDelete        = "DELETE"
)

var (
	ErrInvalid         = errors.New("lookupcatalog: invalid argument")
	ErrNotFound        = errors.New("lookupcatalog: not found")
	ErrConflict        = errors.New("lookupcatalog: conflict")
	ErrCapacity        = errors.New("lookupcatalog: capacity exceeded")
	ErrCorrupt         = errors.New("lookupcatalog: persisted state is corrupt")
	ErrPageInvalidated = errors.New("lookupcatalog: page invalidated")
)

type IDGenerator func() (string, error)

type Options struct {
	Clock       func() time.Time
	IDGenerator IDGenerator
}

type Catalog struct {
	database *sql.DB
	assets   lookupasset.Repository
	clock    func() time.Time
	newID    IDGenerator
}

func New(database *control.DB, assets lookupasset.Repository, options Options) (*Catalog, error) {
	if database == nil || database.SQLDB() == nil || assets == nil {
		return nil, fmt.Errorf("%w: control database and asset repository are required", ErrInvalid)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	newID := options.IDGenerator
	if newID == nil {
		newID = func() (string, error) {
			value := make([]byte, 24)
			if _, err := rand.Read(value); err != nil {
				return "", err
			}
			return "luk_" + base64.RawURLEncoding.EncodeToString(value), nil
		}
	}
	return &Catalog{database: database.SQLDB(), assets: assets, clock: clock, newID: newID}, nil
}

type CreateRequest struct {
	TenantID   string
	OwnerID    string
	Definition *opensplunkv1.LookupDefinition
	Asset      lookupasset.Version
}

type ReplaceRequest struct {
	TenantID        string
	OwnerID         string
	LookupID        string
	ExpectedVersion uint64
	Definition      *opensplunkv1.LookupDefinition
	Asset           lookupasset.Version
}

type GetRequest struct {
	TenantID string
	OwnerID  string
	LookupID string
	Version  uint64
}

type StateRequest struct {
	TenantID        string
	OwnerID         string
	LookupID        string
	ExpectedVersion uint64
	State           opensplunkv1.LookupState
}

type ListRequest struct {
	TenantID string
	OwnerID  string
	AppID    string
	Limit    uint32
}

// ListPageRequest is one normalized management-list query. Position and
// ExpectedSnapshot are detached cursor authority supplied by lookupservice;
// the catalog validates both inside the same SQLite read snapshot as the page.
type ListPageRequest struct {
	TenantID         string
	OwnerID          string
	AppID            string
	States           []opensplunkv1.LookupState
	TextFilter       string
	SortBy           opensplunkv1.LookupSortBy
	SortDirection    opensplunkv1.SortDirection
	Limit            uint32
	Position         *ListPosition
	ExpectedSnapshot *ListSnapshot
	IncludeTotal     bool
}

// ListPosition is the stable keyset immediately after the last returned row.
// Name is populated for name ordering; UnixMicro is populated for timestamp
// ordering. LookupID is the deterministic secondary key for every ordering.
type ListPosition struct {
	LookupID  string
	Name      string
	UnixMicro int64
}

// ListSnapshot is an exact, owner-scoped commitment to every retained logical
// lookup identity and current version. Revision increases for every supported
// mutation; State also detects restore/fork histories with the same revision.
type ListSnapshot struct {
	Revision uint64
	State    [sha256.Size]byte
}

// ListPage is one bounded, detached current-definition page.
type ListPage struct {
	Lookups      []*opensplunkv1.Lookup
	NextPosition *ListPosition
	Snapshot     ListSnapshot
	TotalSize    *uint64
}

type ResolveScope struct {
	TenantID    string
	PrincipalID string
	AppID       string
	Names       []string
}

type Resolved struct {
	Lookup *opensplunkv1.Lookup
	Asset  lookupasset.Version
}

// AdmissionResolution is one atomic explicit-plus-automatic lookup authority.
// Both slices were selected from the same SQLite read snapshot and passed the
// combined stage, cell, and canonical-byte budgets before any asset body load.
type AdmissionResolution struct {
	Explicit  []Resolved
	Automatic []Resolved
}

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type catalogQuerier interface {
	rowQuerier
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type versionLifecycle struct {
	mutationKind string
	state        string
	disabled     sql.NullInt64
	deleted      sql.NullInt64
}

func requireActiveApp(ctx context.Context, query rowQuerier, tenantID, appID string) error {
	if query == nil || !validIdentity(tenantID, 255) || !validIdentity(appID, 128) {
		return fmt.Errorf("%w: lookup app authority is invalid", ErrInvalid)
	}
	var marker int
	err := query.QueryRowContext(ctx, `
		SELECT 1 FROM app_workspaces
		WHERE tenant_id = ? AND app_id = ? AND state = 'active'`,
		tenantID,
		appID,
	).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: lookup app is unavailable", ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("read lookup app authority: %w", err)
	}
	if marker != 1 {
		return ErrCorrupt
	}
	return nil
}

func requireDefinitionCapacity(
	ctx context.Context,
	query rowQuerier,
	tenantID string,
	creating bool,
	terminal bool,
) error {
	if query == nil || !validIdentity(tenantID, 255) {
		return ErrInvalid
	}
	if creating {
		var definitions uint64
		if err := query.QueryRowContext(ctx, `
			SELECT count(*) FROM knowledge_lookup_definitions
			WHERE tenant_id = ?`, tenantID).Scan(&definitions); err != nil {
			return fmt.Errorf("read lookup definition capacity: %w", err)
		}
		if definitions >= MaximumDefinitions {
			return ErrCapacity
		}
	}
	var versions uint64
	if err := query.QueryRowContext(ctx, `
		SELECT count(*) FROM knowledge_lookup_definition_versions
		WHERE tenant_id = ?`, tenantID).Scan(&versions); err != nil {
		return fmt.Errorf("read lookup definition version capacity: %w", err)
	}
	limit := uint64(MaximumOrdinaryVersions)
	if terminal {
		limit = MaximumVersions
	}
	if versions >= limit {
		return ErrCapacity
	}
	return nil
}

func (catalog *Catalog) Create(ctx context.Context, request CreateRequest) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(request.TenantID, 255) || !validIdentity(request.OwnerID, 255) {
		return nil, fmt.Errorf("%w: lookup tenant or owner is invalid", ErrInvalid)
	}
	if err := requireActiveApp(ctx, catalog.database, request.TenantID, request.Definition.GetAppId()); err != nil {
		return nil, err
	}
	normalized, asset, err := catalog.validateDefinitionAsset(ctx, request.TenantID, request.Definition, request.Asset)
	if err != nil {
		return nil, err
	}
	now, err := catalog.now()
	if err != nil {
		return nil, err
	}
	definitionBytes, err := deterministicDefinition(normalized.Definition)
	if err != nil {
		return nil, err
	}
	for range maximumIDAttempts {
		lookupID, generateErr := catalog.newID()
		if generateErr != nil {
			return nil, fmt.Errorf("generate lookup identity: %w", generateErr)
		}
		if !validIdentity(lookupID, 128) {
			return nil, fmt.Errorf("%w: generated lookup identity is invalid", ErrInvalid)
		}
		created, createErr := catalog.createRows(ctx, request.TenantID, request.OwnerID, lookupID, normalized.Definition, definitionBytes, asset, now)
		if createErr == nil {
			return created, nil
		}
		if errors.Is(createErr, ErrConflict) {
			continue
		}
		return nil, createErr
	}
	return nil, fmt.Errorf("%w: generated lookup identities repeatedly collided", ErrConflict)
}

// CreatePublished binds a physical version that was inserted on transaction
// but has not committed yet. It is used only as a lookupasset publication
// finalizer, making the physical version and visible logical definition one
// atomic SQLite commit.
func (catalog *Catalog) CreatePublished(
	ctx context.Context,
	transaction lookupasset.PublicationTransaction,
	request CreateRequest,
) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if transaction == nil || !validIdentity(request.TenantID, 255) ||
		!validIdentity(request.OwnerID, 255) {
		return nil, fmt.Errorf("%w: atomic lookup creation authority is invalid", ErrInvalid)
	}
	if err := requireActiveApp(ctx, transaction, request.TenantID, request.Definition.GetAppId()); err != nil {
		return nil, err
	}
	normalized, asset, err := validatePublishedDefinition(
		ctx,
		request.TenantID,
		request.Definition,
		request.Asset,
	)
	if err != nil {
		return nil, err
	}
	now, err := catalog.now()
	if err != nil {
		return nil, err
	}
	definitionBytes, err := deterministicDefinition(normalized.Definition)
	if err != nil {
		return nil, err
	}
	for range maximumIDAttempts {
		lookupID, generateErr := catalog.newID()
		if generateErr != nil {
			return nil, fmt.Errorf("generate lookup identity: %w", generateErr)
		}
		if !validIdentity(lookupID, 128) {
			return nil, fmt.Errorf("%w: generated lookup identity is invalid", ErrInvalid)
		}
		created, createErr := createRowsInTransaction(
			ctx,
			transaction,
			request.TenantID,
			request.OwnerID,
			lookupID,
			normalized.Definition,
			definitionBytes,
			asset,
			now,
		)
		if createErr == nil {
			return created, nil
		}
		if !errors.Is(createErr, ErrConflict) {
			return nil, createErr
		}
	}
	return nil, fmt.Errorf("%w: lookup identity or logical name repeatedly collided", ErrConflict)
}

func (catalog *Catalog) createRows(ctx context.Context, tenantID, ownerID, lookupID string, definition *opensplunkv1.LookupDefinition, definitionBytes []byte, asset lookupasset.Version, now time.Time) (*opensplunkv1.Lookup, error) {
	tx, err := catalog.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin lookup creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	created, err := createRowsInTransaction(
		ctx,
		tx,
		tenantID,
		ownerID,
		lookupID,
		definition,
		definitionBytes,
		asset,
		now,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lookup creation: %w", err)
	}
	return created, nil
}

func createRowsInTransaction(
	ctx context.Context,
	transaction lookupasset.PublicationTransaction,
	tenantID string,
	ownerID string,
	lookupID string,
	definition *opensplunkv1.LookupDefinition,
	definitionBytes []byte,
	asset lookupasset.Version,
	now time.Time,
) (*opensplunkv1.Lookup, error) {
	// The public non-atomic path performs an early check before parsing the
	// asset, but the app can be archived before this transaction begins. Keep
	// the authoritative check on the same SQLite transaction as publication.
	if err := requireActiveApp(ctx, transaction, tenantID, definition.GetAppId()); err != nil {
		return nil, err
	}
	if err := requireDefinitionCapacity(ctx, transaction, tenantID, true, false); err != nil {
		return nil, err
	}
	_, err := transaction.ExecContext(ctx, `
		INSERT INTO knowledge_lookup_definitions (
			tenant_id, lookup_id, owner_id, app_id, name, sharing_scope, automatic, current_version,
			state, created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'ACTIVE', ?, ?)`,
		tenantID, lookupID, ownerID, definition.GetAppId(), definition.GetName(),
		definition.GetSharingScope(), definition.GetAutomatic(), now.UnixMicro(), now.UnixMicro(),
	)
	if err != nil {
		if storageCapacity(err) {
			return nil, ErrCapacity
		}
		if storageConflict(err) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("insert lookup definition: %w", err)
	}
	if err := insertVersion(
		ctx,
		transaction,
		tenantID,
		lookupID,
		1,
		definitionBytes,
		asset,
		versionLifecycle{mutationKind: mutationCreate, state: "ACTIVE"},
		now,
	); err != nil {
		return nil, err
	}
	return projection(tenantID, ownerID, lookupID, 1, opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE, definition, asset, now, now, time.Time{}, time.Time{}), nil
}

func (catalog *Catalog) Replace(ctx context.Context, request ReplaceRequest) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(request.TenantID, 255) || !validIdentity(request.OwnerID, 255) || !validIdentity(request.LookupID, 128) || request.ExpectedVersion == 0 {
		return nil, fmt.Errorf("%w: lookup replacement authority is invalid", ErrInvalid)
	}
	if err := requireActiveApp(ctx, catalog.database, request.TenantID, request.Definition.GetAppId()); err != nil {
		return nil, err
	}
	normalized, asset, err := catalog.validateDefinitionAsset(ctx, request.TenantID, request.Definition, request.Asset)
	if err != nil {
		return nil, err
	}
	definitionBytes, err := deterministicDefinition(normalized.Definition)
	if err != nil {
		return nil, err
	}
	now, err := catalog.now()
	if err != nil {
		return nil, err
	}
	tx, err := catalog.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin lookup replacement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	replaced, err := replaceRowsInTransaction(
		ctx,
		tx,
		request,
		normalized.Definition,
		definitionBytes,
		asset,
		now,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lookup replacement: %w", err)
	}
	return replaced, nil
}

// ReplacePublished is the replacement counterpart to CreatePublished. The
// newly inserted physical asset becomes reachable from the next logical
// definition version in the same transaction, or neither mutation commits.
func (catalog *Catalog) ReplacePublished(
	ctx context.Context,
	transaction lookupasset.PublicationTransaction,
	request ReplaceRequest,
) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if transaction == nil || !validIdentity(request.TenantID, 255) ||
		!validIdentity(request.OwnerID, 255) || !validIdentity(request.LookupID, 128) ||
		request.ExpectedVersion == 0 {
		return nil, fmt.Errorf("%w: atomic lookup replacement authority is invalid", ErrInvalid)
	}
	if err := requireActiveApp(ctx, transaction, request.TenantID, request.Definition.GetAppId()); err != nil {
		return nil, err
	}
	normalized, asset, err := validatePublishedDefinition(
		ctx,
		request.TenantID,
		request.Definition,
		request.Asset,
	)
	if err != nil {
		return nil, err
	}
	definitionBytes, err := deterministicDefinition(normalized.Definition)
	if err != nil {
		return nil, err
	}
	now, err := catalog.now()
	if err != nil {
		return nil, err
	}
	return replaceRowsInTransaction(
		ctx,
		transaction,
		request,
		normalized.Definition,
		definitionBytes,
		asset,
		now,
	)
}

func replaceRowsInTransaction(
	ctx context.Context,
	transaction lookupasset.PublicationTransaction,
	request ReplaceRequest,
	definition *opensplunkv1.LookupDefinition,
	definitionBytes []byte,
	asset lookupasset.Version,
	now time.Time,
) (*opensplunkv1.Lookup, error) {
	// This must run inside the publication transaction. The early caller check
	// only avoids unnecessary asset validation for an already-archived app.
	if err := requireActiveApp(
		ctx,
		transaction,
		request.TenantID,
		definition.GetAppId(),
	); err != nil {
		return nil, err
	}
	var owner string
	var current uint64
	var state string
	var createdMicro, previousUpdatedMicro int64
	var disabledMicro, deletedMicro sql.NullInt64
	var currentAssetID string
	var currentAssetVersion, currentAssetSize uint64
	var currentAssetDigest, currentDefinitionBytes []byte
	if err := transaction.QueryRowContext(ctx, `
		SELECT registry.owner_id, registry.current_version, registry.state,
			registry.created_at_unix_micro, registry.updated_at_unix_micro,
			registry.disabled_at_unix_micro,
			registry.deleted_at_unix_micro, version.lookup_asset_id,
			version.asset_version, version.asset_size_bytes,
			version.asset_content_sha256, version.definition_proto
		FROM knowledge_lookup_definitions AS registry
		JOIN knowledge_lookup_definition_versions AS version
		  ON version.tenant_id = registry.tenant_id
		 AND version.lookup_id = registry.lookup_id
		 AND version.definition_version = registry.current_version
		WHERE registry.tenant_id = ? AND registry.lookup_id = ?`,
		request.TenantID,
		request.LookupID,
	).Scan(
		&owner,
		&current,
		&state,
		&createdMicro,
		&previousUpdatedMicro,
		&disabledMicro,
		&deletedMicro,
		&currentAssetID,
		&currentAssetVersion,
		&currentAssetSize,
		&currentAssetDigest,
		&currentDefinitionBytes,
	); errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("read lookup replacement authority: %w", err)
	}
	if owner != request.OwnerID || current != request.ExpectedVersion || state == "DELETED" {
		return nil, ErrConflict
	}
	if currentAssetID == asset.Ref.LookupAssetID &&
		currentAssetVersion == asset.Ref.Version &&
		currentAssetSize == asset.Ref.SizeBytes &&
		slices.Equal(currentAssetDigest, asset.Ref.ContentSHA256[:]) &&
		slices.Equal(currentDefinitionBytes, definitionBytes) {
		return nil, ErrConflict
	}
	adjustedNow, err := nextMutationTime(now, previousUpdatedMicro)
	if err != nil {
		return nil, err
	}
	now = adjustedNow
	if err := requireDefinitionCapacity(ctx, transaction, request.TenantID, false, false); err != nil {
		return nil, err
	}
	version := current + 1
	result, err := transaction.ExecContext(ctx, `UPDATE knowledge_lookup_definitions SET app_id = ?, name = ?, sharing_scope = ?, automatic = ?, current_version = ?, updated_at_unix_micro = ? WHERE tenant_id = ? AND lookup_id = ? AND owner_id = ? AND current_version = ? AND state != 'DELETED'`, definition.GetAppId(), definition.GetName(), definition.GetSharingScope(), definition.GetAutomatic(), version, now.UnixMicro(), request.TenantID, request.LookupID, request.OwnerID, current)
	if err != nil {
		if storageConflict(err) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("advance lookup definition: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return nil, ErrConflict
	}
	if err := insertVersion(
		ctx,
		transaction,
		request.TenantID,
		request.LookupID,
		version,
		definitionBytes,
		asset,
		versionLifecycle{
			mutationKind: mutationReplace,
			state:        state,
			disabled:     disabledMicro,
			deleted:      deletedMicro,
		},
		now,
	); err != nil {
		return nil, err
	}
	created := time.UnixMicro(createdMicro).UTC()
	lookupState, disabled, deleted, err := stateTimes(
		state,
		now,
		nullTime(disabledMicro),
		nullTime(deletedMicro),
	)
	if err != nil {
		return nil, err
	}
	return projection(request.TenantID, owner, request.LookupID, version, lookupState, definition, asset, created, now, disabled, deleted), nil
}

func insertVersion(
	ctx context.Context,
	transaction lookupasset.PublicationTransaction,
	tenantID string,
	lookupID string,
	version uint64,
	definitionBytes []byte,
	asset lookupasset.Version,
	lifecycle versionLifecycle,
	now time.Time,
) error {
	columnsBlob, err := encodeColumns(asset.Asset.Headers())
	if err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, `
		INSERT INTO knowledge_lookup_definition_versions (
			tenant_id, lookup_id, definition_version, lookup_asset_id,
			asset_version, asset_size_bytes, asset_content_sha256,
			definition_proto, columns_blob, mutation_kind, state,
			disabled_at_unix_micro, deleted_at_unix_micro,
			created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tenantID,
		lookupID,
		version,
		asset.Ref.LookupAssetID,
		asset.Ref.Version,
		asset.Ref.SizeBytes,
		asset.Ref.ContentSHA256[:],
		definitionBytes,
		columnsBlob,
		lifecycle.mutationKind,
		lifecycle.state,
		nullInt64Value(lifecycle.disabled),
		nullInt64Value(lifecycle.deleted),
		now.UnixMicro(),
	)
	if err != nil {
		if storageCapacity(err) {
			return ErrCapacity
		}
		if storageConflict(err) {
			return ErrConflict
		}
		return fmt.Errorf("insert immutable lookup definition version: %w", err)
	}
	if changed, changedErr := result.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrCorrupt
	}
	return nil
}

func (catalog *Catalog) Get(ctx context.Context, request GetRequest) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	record, err := catalog.getProjectionRecord(
		ctx,
		request.TenantID,
		request.LookupID,
		request.Version,
	)
	if err != nil {
		return nil, err
	}
	if record.lookup.GetOwnerId() != request.OwnerID {
		return nil, ErrNotFound
	}
	return proto.Clone(record.lookup).(*opensplunkv1.Lookup), nil
}

// GetResolved returns an owner-authorized logical projection together with
// the exact immutable physical version it binds. Management orchestration uses
// it to reuse an asset when a metadata-only replacement omits csv_data.
func (catalog *Catalog) GetResolved(ctx context.Context, request GetRequest) (Resolved, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return Resolved{}, err
	}
	resolved, err := catalog.getResolved(ctx, request.TenantID, request.LookupID, request.Version)
	if err != nil {
		return Resolved{}, err
	}
	if resolved.Lookup.GetOwnerId() != request.OwnerID {
		return Resolved{}, ErrNotFound
	}
	return cloneResolved([]Resolved{resolved})[0], nil
}

// SetState applies an optimistic lifecycle transition. The definition version
// is copied immutably so Lookup.version remains a complete mutation token.
func (catalog *Catalog) SetState(ctx context.Context, request StateRequest) (*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(request.TenantID, 255) || !validIdentity(request.OwnerID, 255) || !validIdentity(request.LookupID, 128) || request.ExpectedVersion == 0 ||
		(request.State != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE && request.State != opensplunkv1.LookupState_LOOKUP_STATE_DISABLED && request.State != opensplunkv1.LookupState_LOOKUP_STATE_DELETED) {
		return nil, fmt.Errorf("%w: lookup state authority is invalid", ErrInvalid)
	}
	now, err := catalog.now()
	if err != nil {
		return nil, err
	}
	tx, err := catalog.database.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin lookup state transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var owner, appID, currentState string
	var current uint64
	var previousUpdatedMicro int64
	var currentDisabledMicro sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT owner_id, app_id, current_version, state, updated_at_unix_micro, disabled_at_unix_micro FROM knowledge_lookup_definitions WHERE tenant_id = ? AND lookup_id = ?`, request.TenantID, request.LookupID).Scan(&owner, &appID, &current, &currentState, &previousUpdatedMicro, &currentDisabledMicro); errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("read lookup state authority: %w", err)
	}
	if owner != request.OwnerID || current != request.ExpectedVersion || currentState == "DELETED" ||
		(request.State == opensplunkv1.LookupState_LOOKUP_STATE_DELETED && currentState != "DISABLED") {
		return nil, ErrConflict
	}
	if currentState == "ACTIVE" && request.State == opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE ||
		currentState == "DISABLED" && request.State == opensplunkv1.LookupState_LOOKUP_STATE_DISABLED {
		return nil, ErrConflict
	}
	now, err = nextMutationTime(now, previousUpdatedMicro)
	if err != nil {
		return nil, err
	}
	if request.State == opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE {
		if err := requireActiveApp(ctx, tx, request.TenantID, appID); err != nil {
			return nil, err
		}
	}
	terminal := request.State == opensplunkv1.LookupState_LOOKUP_STATE_DISABLED ||
		request.State == opensplunkv1.LookupState_LOOKUP_STATE_DELETED
	if err := requireDefinitionCapacity(ctx, tx, request.TenantID, false, terminal); err != nil {
		return nil, err
	}
	nextState := "ACTIVE"
	mutationKind := mutationEnable
	disabledValue, deletedValue := any(nil), any(nil)
	switch request.State {
	case opensplunkv1.LookupState_LOOKUP_STATE_DISABLED:
		nextState = "DISABLED"
		mutationKind = mutationDisable
		disabledValue = now.UnixMicro()
	case opensplunkv1.LookupState_LOOKUP_STATE_DELETED:
		if !currentDisabledMicro.Valid {
			return nil, ErrCorrupt
		}
		nextState = "DELETED"
		mutationKind = mutationDelete
		disabledValue = currentDisabledMicro.Int64
		deletedValue = now.UnixMicro()
	}
	version := current + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE knowledge_lookup_definitions
		SET current_version = ?, state = ?, updated_at_unix_micro = ?,
			disabled_at_unix_micro = ?, deleted_at_unix_micro = ?
		WHERE tenant_id = ? AND lookup_id = ? AND owner_id = ?
			AND current_version = ? AND state != 'DELETED'`,
		version, nextState, now.UnixMicro(), disabledValue, deletedValue,
		request.TenantID, request.LookupID, request.OwnerID, current,
	)
	if err != nil {
		if storageCapacity(err) {
			return nil, ErrCapacity
		}
		if storageConflict(err) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("update lookup lifecycle: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return nil, ErrConflict
	}
	result, err = tx.ExecContext(ctx, `
		INSERT INTO knowledge_lookup_definition_versions (
			tenant_id, lookup_id, definition_version, lookup_asset_id,
			asset_version, asset_size_bytes, asset_content_sha256,
			definition_proto, columns_blob, mutation_kind, state,
			disabled_at_unix_micro, deleted_at_unix_micro,
			created_at_unix_micro
		)
		SELECT tenant_id, lookup_id, ?, lookup_asset_id, asset_version,
			asset_size_bytes, asset_content_sha256, definition_proto, columns_blob,
			?, ?, ?, ?, ?
		FROM knowledge_lookup_definition_versions
		WHERE tenant_id = ? AND lookup_id = ? AND definition_version = ?`,
		version, mutationKind, nextState, disabledValue, deletedValue,
		now.UnixMicro(), request.TenantID, request.LookupID, current,
	)
	if err != nil {
		if storageCapacity(err) {
			return nil, ErrCapacity
		}
		if storageConflict(err) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("copy lookup definition lifecycle version: %w", err)
	}
	if changed, changedErr := result.RowsAffected(); changedErr != nil || changed != 1 {
		return nil, ErrCorrupt
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lookup state transition: %w", err)
	}
	return catalog.Get(ctx, GetRequest{TenantID: request.TenantID, OwnerID: request.OwnerID, LookupID: request.LookupID, Version: version})
}

func (catalog *Catalog) List(ctx context.Context, request ListRequest) ([]*opensplunkv1.Lookup, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(request.TenantID, 255) || !validIdentity(request.OwnerID, 255) || (request.AppID != "" && !validIdentity(request.AppID, 128)) || request.Limit == 0 || request.Limit > MaximumManagedLookups+1 {
		return nil, fmt.Errorf("%w: lookup list scope is invalid", ErrInvalid)
	}
	page, err := catalog.ListPage(ctx, ListPageRequest{
		TenantID:      request.TenantID,
		OwnerID:       request.OwnerID,
		AppID:         request.AppID,
		SortBy:        opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_NAME,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		Limit:         request.Limit,
	})
	if err != nil {
		return nil, err
	}
	return page.Lookups, nil
}

func (catalog *Catalog) Resolve(ctx context.Context, scope ResolveScope) ([]Resolved, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(scope.TenantID, 255) || !validIdentity(scope.PrincipalID, 255) || !validIdentity(scope.AppID, 128) || len(scope.Names) > MaximumResolvedLookups {
		return nil, fmt.Errorf("%w: lookup resolution scope is invalid", ErrInvalid)
	}
	tx, err := catalog.database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin lookup resolution snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	records := make([]persistedProjection, 0, len(scope.Names))
	for _, name := range scope.Names {
		if !lookupdefinition.IsValidLookupName(name) {
			return nil, fmt.Errorf("%w: lookup name is invalid", ErrInvalid)
		}
		lookupID, err := catalog.resolutionWinnerID(ctx, tx, scope, name)
		if err != nil {
			return nil, err
		}
		record, recordErr := catalog.getProjectionRecordWith(
			ctx,
			tx,
			scope.TenantID,
			lookupID,
			0,
		)
		if recordErr != nil {
			return nil, recordErr
		}
		if record.lookup.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE ||
			record.lookup.GetDefinition().GetName() != name {
			return nil, ErrConflict
		}
		records = append(records, record)
	}
	if err := preflightResolvedRecords(records); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lookup resolution snapshot: %w", err)
	}
	result := make([]Resolved, len(records))
	for index, record := range records {
		resolved, resolveErr := catalog.resolvePersistedProjection(ctx, record)
		if resolveErr != nil {
			return nil, resolveErr
		}
		result[index] = resolved
	}
	return cloneResolved(result), nil
}

// ResolveAdmission selects explicit names and automatic winners from one read
// snapshot, applies the combined execution budgets to metadata, and only then
// loads immutable CSV bodies. Explicit order follows scope.Names; automatic
// order is canonical definition name then stable ID.
func (catalog *Catalog) ResolveAdmission(
	ctx context.Context,
	scope ResolveScope,
) (AdmissionResolution, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return AdmissionResolution{}, err
	}
	if !validIdentity(scope.TenantID, 255) || !validIdentity(scope.PrincipalID, 255) ||
		!validIdentity(scope.AppID, 128) || len(scope.Names) > MaximumResolvedLookups {
		return AdmissionResolution{}, fmt.Errorf("%w: lookup admission resolution scope is invalid", ErrInvalid)
	}
	tx, err := catalog.database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AdmissionResolution{}, fmt.Errorf("begin lookup admission resolution snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	explicitRecords := make([]persistedProjection, 0, len(scope.Names))
	for _, name := range scope.Names {
		if !lookupdefinition.IsValidLookupName(name) {
			return AdmissionResolution{}, fmt.Errorf("%w: lookup name is invalid", ErrInvalid)
		}
		lookupID, winnerErr := catalog.resolutionWinnerID(ctx, tx, scope, name)
		if winnerErr != nil {
			return AdmissionResolution{}, winnerErr
		}
		record, recordErr := catalog.getProjectionRecordWith(ctx, tx, scope.TenantID, lookupID, 0)
		if recordErr != nil {
			return AdmissionResolution{}, recordErr
		}
		if record.lookup.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE ||
			record.lookup.GetDefinition().GetName() != name {
			return AdmissionResolution{}, ErrConflict
		}
		explicitRecords = append(explicitRecords, record)
	}
	automaticRecords, err := catalog.resolveAutomaticProjectionRecords(
		ctx,
		tx,
		scope,
		MaximumResolvedLookups-len(explicitRecords),
	)
	if err != nil {
		return AdmissionResolution{}, err
	}
	allRecords := make([]persistedProjection, 0, len(explicitRecords)+len(automaticRecords))
	allRecords = append(allRecords, explicitRecords...)
	allRecords = append(allRecords, automaticRecords...)
	if err := preflightResolvedRecords(allRecords); err != nil {
		return AdmissionResolution{}, err
	}
	if err := tx.Commit(); err != nil {
		return AdmissionResolution{}, fmt.Errorf("commit lookup admission resolution snapshot: %w", err)
	}
	resolved, err := catalog.resolvePersistedRecords(ctx, allRecords)
	if err != nil {
		return AdmissionResolution{}, err
	}
	explicitCount := len(explicitRecords)
	return AdmissionResolution{
		Explicit:  resolved[:explicitCount],
		Automatic: resolved[explicitCount:],
	}, nil
}

// ResolveAutomatic selects the same visible logical-name winners as explicit
// resolution, then returns only winners marked automatic. Registry metadata
// decides visibility and precedence before at most sixteen immutable assets
// are loaded. Result order is canonical name then stable ID.
func (catalog *Catalog) ResolveAutomatic(ctx context.Context, scope ResolveScope) ([]Resolved, error) {
	if err := catalog.validateContext(ctx); err != nil {
		return nil, err
	}
	if !validIdentity(scope.TenantID, 255) || !validIdentity(scope.PrincipalID, 255) ||
		!validIdentity(scope.AppID, 128) || len(scope.Names) != 0 {
		return nil, fmt.Errorf("%w: automatic lookup resolution scope is invalid", ErrInvalid)
	}
	tx, err := catalog.database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin automatic lookup resolution snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	records, err := catalog.resolveAutomaticProjectionRecords(
		ctx,
		tx,
		scope,
		MaximumResolvedLookups,
	)
	if err != nil {
		return nil, err
	}
	if err := preflightResolvedRecords(records); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit automatic lookup resolution snapshot: %w", err)
	}
	return catalog.resolvePersistedRecords(ctx, records)
}

func (catalog *Catalog) resolveAutomaticProjectionRecords(
	ctx context.Context,
	query catalogQuerier,
	scope ResolveScope,
	maximum int,
) ([]persistedProjection, error) {
	if query == nil || maximum < 0 || maximum > MaximumResolvedLookups {
		return nil, ErrInvalid
	}
	const visible = `
		SELECT definition.lookup_id, definition.name,
			definition.sharing_scope - 1 AS precedence_rank,
			definition.automatic
		FROM knowledge_lookup_definitions AS definition
		JOIN app_workspaces AS visible_app
		  ON visible_app.tenant_id = definition.tenant_id
		 AND visible_app.app_id = definition.app_id
		 AND visible_app.state = 'active'
		WHERE definition.tenant_id = ?
		  AND definition.state = 'ACTIVE'
		  AND (
		    (definition.sharing_scope = 1 AND definition.owner_id = ? AND definition.app_id = ?)
		    OR (definition.sharing_scope = 2 AND definition.app_id = ?)
		    OR definition.sharing_scope = 3
		  )`
	arguments := []any{
		scope.TenantID,
		scope.PrincipalID,
		scope.AppID,
		scope.AppID,
	}
	var visibleCount, maximumWinningMatches int64
	if err := query.QueryRowContext(ctx, `
		WITH visible AS (`+visible+`),
		ranked AS (
			SELECT precedence_rank,
				min(precedence_rank) OVER (PARTITION BY name) AS winning_rank,
				count(*) OVER (PARTITION BY name, precedence_rank) AS rank_matches
			FROM visible
		)
		SELECT (SELECT count(*) FROM visible),
			COALESCE(max(CASE WHEN precedence_rank = winning_rank THEN rank_matches ELSE 0 END), 0)
		FROM ranked`, arguments...).Scan(&visibleCount, &maximumWinningMatches); err != nil {
		return nil, fmt.Errorf("preflight automatic lookup winners: %w", err)
	}
	if visibleCount < 0 || visibleCount > MaximumManagedLookups {
		return nil, ErrCapacity
	}
	if maximumWinningMatches > 1 {
		return nil, ErrConflict
	}
	if maximumWinningMatches < 0 {
		return nil, ErrCorrupt
	}
	rows, err := query.QueryContext(ctx, `
		WITH visible AS (`+visible+`),
		winners AS (
			SELECT lookup_id, name, automatic, precedence_rank,
				min(precedence_rank) OVER (PARTITION BY name) AS winning_rank
			FROM visible
		)
		SELECT `+projectionSelectColumns+`
		FROM winners
		JOIN knowledge_lookup_definitions AS registry
		  ON registry.tenant_id = ?
		 AND registry.lookup_id = winners.lookup_id
		JOIN knowledge_lookup_definition_versions AS version
		  ON version.tenant_id = registry.tenant_id
		 AND version.lookup_id = registry.lookup_id
		 AND version.definition_version = registry.current_version
		JOIN knowledge_lookup_asset_versions AS asset
		  ON asset.tenant_id = version.tenant_id
		 AND asset.lookup_asset_id = version.lookup_asset_id
		 AND asset.asset_version = version.asset_version
		 AND asset.content_sha256 = version.asset_content_sha256
		 AND asset.canonical_bytes = version.asset_size_bytes
		WHERE winners.precedence_rank = winners.winning_rank
		  AND winners.automatic = 1
		ORDER BY winners.name, winners.lookup_id
		LIMIT ?`, append(arguments, scope.TenantID, maximum+1)...)
	if err != nil {
		return nil, fmt.Errorf("read automatic lookup winners: %w", err)
	}
	records := make([]persistedProjection, 0, maximum+1)
	for rows.Next() {
		registry, version, scanErr := scanJoinedProjection(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan automatic lookup winners: %w", scanErr)
		}
		record, projectionErr := projectPersisted(registry, version)
		if projectionErr != nil {
			_ = rows.Close()
			return nil, projectionErr
		}
		if record.lookup.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE ||
			!record.lookup.GetDefinition().GetAutomatic() ||
			record.lookup.GetDefinition().GetName() != registry.name {
			_ = rows.Close()
			return nil, ErrConflict
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate automatic lookup winners: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close automatic lookup winners: %w", err)
	}
	if len(records) > maximum {
		return nil, ErrCapacity
	}
	return records, nil
}

func (catalog *Catalog) resolvePersistedRecords(
	ctx context.Context,
	records []persistedProjection,
) ([]Resolved, error) {
	result := make([]Resolved, len(records))
	assets := make(map[lookupasset.VersionRef]lookupasset.Version, len(records))
	for index, record := range records {
		asset, loaded := assets[record.assetRef]
		if !loaded {
			var err error
			asset, err = catalog.assets.GetVersion(ctx, record.assetRef)
			if err != nil {
				return nil, fmt.Errorf("resolve lookup asset: %w", err)
			}
			assets[record.assetRef] = asset
		}
		resolved, err := resolvePersistedProjectionAsset(record, asset)
		if err != nil {
			return nil, err
		}
		result[index] = resolved
	}
	return cloneResolved(result), nil
}

func (catalog *Catalog) resolutionWinnerID(
	ctx context.Context,
	query catalogQuerier,
	scope ResolveScope,
	name string,
) (string, error) {
	if query == nil {
		return "", ErrInvalid
	}
	rows, err := query.QueryContext(ctx, `
		SELECT definition.lookup_id, definition.sharing_scope
		FROM knowledge_lookup_definitions AS definition
		JOIN app_workspaces AS app
		  ON app.tenant_id = definition.tenant_id
		 AND app.app_id = definition.app_id
		 AND app.state = 'active'
		WHERE definition.tenant_id = ?
		  AND definition.name = ?
		  AND definition.state = 'ACTIVE'
		  AND (
		    (definition.sharing_scope = 1 AND definition.owner_id = ? AND definition.app_id = ?)
		    OR (definition.sharing_scope = 2 AND definition.app_id = ?)
		    OR definition.sharing_scope = 3
		  )
		ORDER BY definition.lookup_id LIMIT 4`,
		scope.TenantID,
		name,
		scope.PrincipalID,
		scope.AppID,
		scope.AppID,
	)
	if err != nil {
		return "", fmt.Errorf("resolve lookup %q: %w", name, err)
	}
	defer rows.Close()
	lookupID := ""
	bestRank := 4
	matches := 0
	for rows.Next() {
		var id string
		var sharing int32
		if scanErr := rows.Scan(&id, &sharing); scanErr != nil {
			return "", fmt.Errorf("resolve lookup %q: %w", name, scanErr)
		}
		rank := sharingRank(sharing)
		if rank < 0 {
			return "", ErrCorrupt
		}
		if rank < bestRank {
			lookupID, bestRank, matches = id, rank, 1
		} else if rank == bestRank {
			matches++
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("resolve lookup %q: %w", name, err)
	}
	if matches == 0 {
		return "", ErrNotFound
	}
	if matches != 1 {
		return "", ErrConflict
	}
	return lookupID, nil
}

func sharingRank(value int32) int {
	switch opensplunkv1.SharingScope(value) {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return 0
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return 1
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return 2
	default:
		return -1
	}
}

type persistedProjection struct {
	lookup   *opensplunkv1.Lookup
	assetRef lookupasset.VersionRef
}

type persistedRegistry struct {
	tenantID       string
	lookupID       string
	ownerID        string
	appID          string
	name           string
	sharingScope   int32
	automatic      bool
	currentVersion uint64
	state          string
	createdMicro   int64
	updatedMicro   int64
	disabledMicro  sql.NullInt64
	deletedMicro   sql.NullInt64
}

type persistedDefinitionVersion struct {
	definitionVersion uint64
	assetID           string
	assetVersion      uint64
	assetSize         uint64
	digestBytes       []byte
	definitionBytes   []byte
	columnsBlob       []byte
	mutationKind      string
	state             string
	disabledMicro     sql.NullInt64
	deletedMicro      sql.NullInt64
	createdMicro      int64
	sourceDigestBytes []byte
	rowCount          uint64
	columnCount       uint32
}

type projectionScanner interface {
	Scan(...any) error
}

func scanJoinedProjection(scanner projectionScanner) (
	persistedRegistry,
	persistedDefinitionVersion,
	error,
) {
	registry := persistedRegistry{}
	version := persistedDefinitionVersion{}
	err := scanner.Scan(
		&registry.tenantID,
		&registry.lookupID,
		&registry.ownerID,
		&registry.appID,
		&registry.name,
		&registry.sharingScope,
		&registry.automatic,
		&registry.currentVersion,
		&registry.state,
		&registry.createdMicro,
		&registry.updatedMicro,
		&registry.disabledMicro,
		&registry.deletedMicro,
		&version.definitionVersion,
		&version.assetID,
		&version.assetVersion,
		&version.assetSize,
		&version.digestBytes,
		&version.definitionBytes,
		&version.columnsBlob,
		&version.mutationKind,
		&version.state,
		&version.disabledMicro,
		&version.deletedMicro,
		&version.createdMicro,
		&version.sourceDigestBytes,
		&version.rowCount,
		&version.columnCount,
	)
	return registry, version, err
}

func projectPersisted(
	registry persistedRegistry,
	version persistedDefinitionVersion,
) (persistedProjection, error) {
	if err := validatePersistedRegistry(registry); err != nil {
		return persistedProjection{}, err
	}
	if version.definitionVersion == 0 || version.definitionVersion > ^uint64(0)>>1 ||
		!validPersistedIdentity(version.assetID, 128) || version.assetVersion == 0 ||
		version.assetVersion > ^uint64(0)>>1 || version.assetSize == 0 ||
		version.assetSize > lookupasset.MaximumSourceBytes ||
		len(version.digestBytes) != sha256.Size || len(version.sourceDigestBytes) != sha256.Size ||
		version.rowCount > lookupasset.MaximumRows || version.columnCount == 0 ||
		version.columnCount > lookupasset.MaximumColumns ||
		version.createdMicro < registry.createdMicro ||
		version.createdMicro > registry.updatedMicro || version.createdMicro > maximumUnixMicro {
		return persistedProjection{}, ErrCorrupt
	}
	if !validOptionalTimestamp(version.disabledMicro, 1, version.createdMicro) ||
		!validOptionalTimestamp(version.deletedMicro, 1, version.createdMicro) {
		return persistedProjection{}, ErrCorrupt
	}
	var digest [sha256.Size]byte
	copy(digest[:], version.digestBytes)
	var sourceDigest [sha256.Size]byte
	copy(sourceDigest[:], version.sourceDigestBytes)
	if digest == ([sha256.Size]byte{}) || sourceDigest == ([sha256.Size]byte{}) {
		return persistedProjection{}, ErrCorrupt
	}
	columns, err := decodeColumns(version.columnsBlob)
	if err != nil || len(columns) != int(version.columnCount) {
		return persistedProjection{}, ErrCorrupt
	}
	definition := &opensplunkv1.LookupDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(version.definitionBytes, definition); err != nil {
		return persistedProjection{}, ErrCorrupt
	}
	normalized, err := lookupdefinition.Normalize(definition, columns)
	if err != nil {
		return persistedProjection{}, ErrCorrupt
	}
	canonical, err := deterministicDefinition(normalized.Definition)
	if err != nil || !slices.Equal(canonical, version.definitionBytes) ||
		(version.definitionVersion == registry.currentVersion &&
			(registry.appID != normalized.Definition.GetAppId() ||
				registry.name != normalized.Definition.GetName() ||
				registry.sharingScope != int32(normalized.Definition.GetSharingScope()) ||
				registry.automatic != normalized.Definition.GetAutomatic() ||
				registry.state != version.state ||
				registry.updatedMicro != version.createdMicro ||
				!equalNullInt64(registry.disabledMicro, version.disabledMicro) ||
				!equalNullInt64(registry.deletedMicro, version.deletedMicro))) {
		return persistedProjection{}, ErrCorrupt
	}
	lookupState, disabled, deleted, err := versionStateTimes(
		version.mutationKind,
		version.state,
		version.createdMicro,
		version.disabledMicro,
		version.deletedMicro,
	)
	if err != nil {
		return persistedProjection{}, err
	}
	lookup := projectionMetadata(
		registry.tenantID,
		registry.ownerID,
		registry.lookupID,
		version.definitionVersion,
		lookupState,
		normalized.Definition,
		columns,
		version.rowCount,
		version.assetSize,
		sourceDigest,
		digest,
		time.UnixMicro(registry.createdMicro).UTC(),
		time.UnixMicro(version.createdMicro).UTC(),
		disabled,
		deleted,
	)
	return persistedProjection{
		lookup: lookup,
		assetRef: lookupasset.VersionRef{
			TenantID:      registry.tenantID,
			LookupAssetID: version.assetID,
			Version:       version.assetVersion,
			SizeBytes:     version.assetSize,
			ContentSHA256: digest,
		},
	}, nil
}

func validatePersistedRegistry(registry persistedRegistry) error {
	if !validIdentity(registry.tenantID, 255) ||
		!validIdentity(registry.lookupID, 128) ||
		!validIdentity(registry.ownerID, 255) ||
		!validIdentity(registry.appID, 128) ||
		!lookupdefinition.IsValidLookupName(registry.name) ||
		sharingRank(registry.sharingScope) < 0 || registry.currentVersion == 0 ||
		registry.currentVersion > ^uint64(0)>>1 {
		return ErrCorrupt
	}
	if registry.createdMicro < 1 || registry.createdMicro > maximumUnixMicro ||
		registry.updatedMicro < registry.createdMicro || registry.updatedMicro > maximumUnixMicro ||
		!validOptionalTimestamp(registry.disabledMicro, registry.createdMicro, registry.updatedMicro) ||
		!validOptionalTimestamp(registry.deletedMicro, registry.createdMicro, registry.updatedMicro) {
		return ErrCorrupt
	}
	if _, _, _, err := stateTimes(
		registry.state,
		time.Time{},
		nullTime(registry.disabledMicro),
		nullTime(registry.deletedMicro),
	); err != nil {
		return err
	}
	return nil
}

func preflightResolvedRecords(records []persistedProjection) error {
	if len(records) > MaximumResolvedLookups {
		return ErrCapacity
	}
	var cells, canonicalBytes uint64
	for _, record := range records {
		if record.lookup == nil || len(record.lookup.GetColumns()) == 0 {
			return ErrCorrupt
		}
		rowCount := record.lookup.GetRowCount()
		columnCount := uint64(len(record.lookup.GetColumns()))
		if rowCount > lookupasset.MaximumRows || columnCount > lookupasset.MaximumColumns {
			return ErrCorrupt
		}
		itemCells := rowCount * columnCount
		if itemCells > MaximumResolvedCells-cells {
			return ErrCapacity
		}
		cells += itemCells
		itemBytes := record.lookup.GetCanonicalSizeBytes()
		if itemBytes == 0 || itemBytes > lookupasset.MaximumSourceBytes ||
			itemBytes > MaximumResolvedBytes-canonicalBytes {
			return ErrCapacity
		}
		canonicalBytes += itemBytes
	}
	return nil
}

func (catalog *Catalog) getProjectionRecord(
	ctx context.Context,
	tenantID string,
	lookupID string,
	requestedVersion uint64,
) (persistedProjection, error) {
	return catalog.getProjectionRecordWith(
		ctx,
		catalog.database,
		tenantID,
		lookupID,
		requestedVersion,
	)
}

func (catalog *Catalog) getProjectionRecordWith(
	ctx context.Context,
	query rowQuerier,
	tenantID string,
	lookupID string,
	requestedVersion uint64,
) (persistedProjection, error) {
	if !validIdentity(tenantID, 255) || !validIdentity(lookupID, 128) ||
		requestedVersion > ^uint64(0)>>1 {
		return persistedProjection{}, fmt.Errorf("%w: lookup identity is invalid", ErrInvalid)
	}
	if query == nil {
		return persistedProjection{}, ErrInvalid
	}
	registry := persistedRegistry{tenantID: tenantID, lookupID: lookupID}
	if err := query.QueryRowContext(ctx, `SELECT owner_id, app_id, name, sharing_scope, automatic, current_version, state, created_at_unix_micro, updated_at_unix_micro, disabled_at_unix_micro, deleted_at_unix_micro FROM knowledge_lookup_definitions WHERE tenant_id = ? AND lookup_id = ?`, tenantID, lookupID).Scan(&registry.ownerID, &registry.appID, &registry.name, &registry.sharingScope, &registry.automatic, &registry.currentVersion, &registry.state, &registry.createdMicro, &registry.updatedMicro, &registry.disabledMicro, &registry.deletedMicro); errors.Is(err, sql.ErrNoRows) {
		return persistedProjection{}, ErrNotFound
	} else if err != nil {
		return persistedProjection{}, fmt.Errorf("read lookup registry: %w", err)
	}
	if err := validatePersistedRegistry(registry); err != nil {
		return persistedProjection{}, err
	}
	version := requestedVersion
	if version == 0 {
		version = registry.currentVersion
	}
	persistedVersion := persistedDefinitionVersion{definitionVersion: version}
	if err := query.QueryRowContext(ctx, `
		SELECT version.lookup_asset_id, version.asset_version,
			version.asset_size_bytes, version.asset_content_sha256,
			version.definition_proto, version.columns_blob, version.mutation_kind,
			version.state, version.disabled_at_unix_micro,
			version.deleted_at_unix_micro, version.created_at_unix_micro,
			asset.source_sha256, asset.row_count, asset.column_count
		FROM knowledge_lookup_definition_versions AS version
		JOIN knowledge_lookup_asset_versions AS asset
		  ON asset.tenant_id = version.tenant_id
		 AND asset.lookup_asset_id = version.lookup_asset_id
		 AND asset.asset_version = version.asset_version
		 AND asset.content_sha256 = version.asset_content_sha256
		 AND asset.canonical_bytes = version.asset_size_bytes
		WHERE version.tenant_id = ? AND version.lookup_id = ?
		  AND version.definition_version = ?`,
		tenantID,
		lookupID,
		version,
	).Scan(
		&persistedVersion.assetID,
		&persistedVersion.assetVersion,
		&persistedVersion.assetSize,
		&persistedVersion.digestBytes,
		&persistedVersion.definitionBytes,
		&persistedVersion.columnsBlob,
		&persistedVersion.mutationKind,
		&persistedVersion.state,
		&persistedVersion.disabledMicro,
		&persistedVersion.deletedMicro,
		&persistedVersion.createdMicro,
		&persistedVersion.sourceDigestBytes,
		&persistedVersion.rowCount,
		&persistedVersion.columnCount,
	); errors.Is(err, sql.ErrNoRows) {
		return persistedProjection{}, ErrNotFound
	} else if err != nil {
		return persistedProjection{}, fmt.Errorf("read lookup definition version: %w", err)
	}
	return projectPersisted(registry, persistedVersion)
}

func (catalog *Catalog) getResolved(ctx context.Context, tenantID, lookupID string, requestedVersion uint64) (Resolved, error) {
	record, err := catalog.getProjectionRecord(ctx, tenantID, lookupID, requestedVersion)
	if err != nil {
		return Resolved{}, err
	}
	return catalog.resolvePersistedProjection(ctx, record)
}

func (catalog *Catalog) resolvePersistedProjection(
	ctx context.Context,
	record persistedProjection,
) (Resolved, error) {
	if record.lookup == nil {
		return Resolved{}, ErrCorrupt
	}
	asset, err := catalog.assets.GetVersion(ctx, record.assetRef)
	if err != nil {
		return Resolved{}, fmt.Errorf("resolve lookup asset: %w", err)
	}
	return resolvePersistedProjectionAsset(record, asset)
}

func resolvePersistedProjectionAsset(
	record persistedProjection,
	asset lookupasset.Version,
) (Resolved, error) {
	if record.lookup == nil {
		return Resolved{}, ErrCorrupt
	}
	if asset.Ref != record.assetRef || asset.Asset == nil ||
		asset.Asset.CanonicalSizeBytes() != asset.Ref.SizeBytes ||
		asset.Asset.ContentSHA256() != asset.Ref.ContentSHA256 ||
		asset.Asset.SourceSHA256() != asset.SourceSHA256 ||
		!slices.Equal(asset.Asset.Headers(), record.lookup.GetColumns()) ||
		asset.Asset.RowCount() != record.lookup.GetRowCount() ||
		!slices.Equal(asset.SourceSHA256[:], record.lookup.GetSourceSha256()) {
		return Resolved{}, ErrCorrupt
	}
	return Resolved{Lookup: record.lookup, Asset: asset}, nil
}

func (catalog *Catalog) validateDefinitionAsset(ctx context.Context, tenantID string, definition *opensplunkv1.LookupDefinition, supplied lookupasset.Version) (lookupdefinition.Normalized, lookupasset.Version, error) {
	if supplied.Ref.TenantID != tenantID {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, fmt.Errorf("%w: asset tenant does not match definition tenant", ErrInvalid)
	}
	asset, err := catalog.assets.GetVersion(ctx, supplied.Ref)
	if err != nil {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, err
	}
	normalized, err := lookupdefinition.Normalize(definition, asset.Asset.Headers())
	if err != nil {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, err
	}
	keyColumns := make([]string, len(normalized.Definition.GetKeyMappings()))
	for index, mapping := range normalized.Definition.GetKeyMappings() {
		keyColumns[index] = mapping.GetLookupField()
	}
	if err := lookupasset.ValidateUniqueKeysContext(ctx, asset.Asset, keyColumns); err != nil {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, err
	}
	return normalized, asset, nil
}

func validatePublishedDefinition(
	ctx context.Context,
	tenantID string,
	definition *opensplunkv1.LookupDefinition,
	asset lookupasset.Version,
) (lookupdefinition.Normalized, lookupasset.Version, error) {
	if asset.Ref.TenantID != tenantID || asset.Asset == nil ||
		!validIdentity(asset.Ref.LookupAssetID, 128) || asset.Ref.Version == 0 ||
		asset.Ref.Version > ^uint64(0)>>1 ||
		asset.Ref.SizeBytes == 0 || asset.Ref.SizeBytes > lookupasset.MaximumSourceBytes ||
		asset.Ref.ContentSHA256 == ([sha256.Size]byte{}) ||
		asset.SourceSHA256 == ([sha256.Size]byte{}) ||
		asset.SourceBytes == 0 || asset.SourceBytes > lookupasset.MaximumSourceBytes ||
		asset.CreatedAt.UnixMicro() < 1 || asset.CreatedAt.UnixMicro() > maximumUnixMicro {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, fmt.Errorf(
			"%w: uncommitted lookup asset authority is invalid",
			ErrInvalid,
		)
	}
	if asset.Asset.CanonicalSizeBytes() != asset.Ref.SizeBytes ||
		asset.Asset.ContentSHA256() != asset.Ref.ContentSHA256 ||
		asset.Asset.SourceSHA256() != asset.SourceSHA256 ||
		asset.Asset.SourceBytes() != asset.SourceBytes {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, fmt.Errorf(
			"%w: uncommitted lookup asset metadata disagrees",
			ErrInvalid,
		)
	}
	normalized, err := lookupdefinition.Normalize(definition, asset.Asset.Headers())
	if err != nil {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, err
	}
	keyColumns := make([]string, len(normalized.Definition.GetKeyMappings()))
	for index, mapping := range normalized.Definition.GetKeyMappings() {
		keyColumns[index] = mapping.GetLookupField()
	}
	if err := lookupasset.ValidateUniqueKeysContext(ctx, asset.Asset, keyColumns); err != nil {
		return lookupdefinition.Normalized{}, lookupasset.Version{}, err
	}
	return normalized, asset, nil
}

func deterministicDefinition(definition *opensplunkv1.LookupDefinition) ([]byte, error) {
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(definition)
	if err != nil || len(bytes) == 0 || len(bytes) > 64<<10 {
		return nil, fmt.Errorf("%w: lookup definition wire form is invalid", ErrInvalid)
	}
	return bytes, nil
}

func encodeColumns(columns []string) ([]byte, error) {
	if len(columns) == 0 || len(columns) > lookupasset.MaximumColumns {
		return nil, fmt.Errorf("%w: lookup column inventory is invalid", ErrInvalid)
	}
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(len(columns))) // #nosec G115 -- columns are bounded above.
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if !validPersistedColumn(column) {
			return nil, fmt.Errorf("%w: lookup column inventory is invalid", ErrInvalid)
		}
		if _, duplicate := seen[column]; duplicate {
			return nil, fmt.Errorf("%w: lookup column inventory is duplicated", ErrInvalid)
		}
		seen[column] = struct{}{}
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(column))) // #nosec G115 -- header bytes are bounded above.
		result = append(result, length...)
		result = append(result, column...)
	}
	return result, nil
}

func decodeColumns(encoded []byte) ([]string, error) {
	if len(encoded) < 4 {
		return nil, ErrCorrupt
	}
	count := binary.BigEndian.Uint32(encoded[:4])
	if count == 0 || count > lookupasset.MaximumColumns {
		return nil, ErrCorrupt
	}
	result := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	remaining := encoded[4:]
	for range count {
		if len(remaining) < 4 {
			return nil, ErrCorrupt
		}
		length := binary.BigEndian.Uint32(remaining[:4])
		remaining = remaining[4:]
		if length == 0 || length > lookupasset.MaximumHeaderBytes || uint64(length) > uint64(len(remaining)) {
			return nil, ErrCorrupt
		}
		column := string(remaining[:length])
		remaining = remaining[length:]
		if !validPersistedColumn(column) {
			return nil, ErrCorrupt
		}
		if _, duplicate := seen[column]; duplicate {
			return nil, ErrCorrupt
		}
		seen[column] = struct{}{}
		result = append(result, column)
	}
	if len(remaining) != 0 {
		return nil, ErrCorrupt
	}
	return result, nil
}

func validPersistedColumn(value string) bool {
	return value != "" && len(value) <= lookupasset.MaximumHeaderBytes &&
		utf8.ValidString(value) && strings.TrimSpace(value) == value &&
		strings.IndexByte(value, 0) < 0 && !strings.ContainsFunc(value, func(character rune) bool {
		return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
	})
}

func validPersistedIdentity(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validOptionalTimestamp(value sql.NullInt64, created, updated int64) bool {
	return !value.Valid || value.Int64 >= created && value.Int64 <= updated
}

func equalNullInt64(left, right sql.NullInt64) bool {
	return left.Valid == right.Valid && (!left.Valid || left.Int64 == right.Int64)
}

func versionStateTimes(
	mutationKind string,
	state string,
	createdMicro int64,
	disabledMicro sql.NullInt64,
	deletedMicro sql.NullInt64,
) (opensplunkv1.LookupState, time.Time, time.Time, error) {
	lookupState, disabled, deleted, err := stateTimes(
		state,
		time.Time{},
		nullTime(disabledMicro),
		nullTime(deletedMicro),
	)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	valid := false
	switch mutationKind {
	case mutationCreate:
		valid = state == "ACTIVE"
	case mutationReplace:
		valid = state == "ACTIVE" || state == "DISABLED"
	case mutationEnable:
		valid = state == "ACTIVE"
	case mutationDisable:
		valid = state == "DISABLED" && disabledMicro.Valid &&
			disabledMicro.Int64 == createdMicro
	case mutationDelete:
		valid = state == "DELETED" && disabledMicro.Valid &&
			deletedMicro.Valid && deletedMicro.Int64 == createdMicro
	}
	if !valid {
		return 0, time.Time{}, time.Time{}, ErrCorrupt
	}
	return lookupState, disabled, deleted, nil
}

func projection(tenantID, ownerID, lookupID string, version uint64, state opensplunkv1.LookupState, definition *opensplunkv1.LookupDefinition, asset lookupasset.Version, created, updated, disabled, deleted time.Time) *opensplunkv1.Lookup {
	return projectionMetadata(
		tenantID,
		ownerID,
		lookupID,
		version,
		state,
		definition,
		asset.Asset.Headers(),
		asset.Asset.RowCount(),
		asset.Ref.SizeBytes,
		asset.SourceSHA256,
		asset.Ref.ContentSHA256,
		created,
		updated,
		disabled,
		deleted,
	)
}

func projectionMetadata(
	tenantID string,
	ownerID string,
	lookupID string,
	version uint64,
	state opensplunkv1.LookupState,
	definition *opensplunkv1.LookupDefinition,
	columns []string,
	rowCount uint64,
	canonicalSize uint64,
	sourceDigest [sha256.Size]byte,
	contentDigest [sha256.Size]byte,
	created time.Time,
	updated time.Time,
	disabled time.Time,
	deleted time.Time,
) *opensplunkv1.Lookup {
	result := &opensplunkv1.Lookup{
		LookupId: lookupID, TenantId: tenantID, OwnerId: ownerID, Version: version, State: state,
		Definition: proto.Clone(definition).(*opensplunkv1.LookupDefinition), Columns: slices.Clone(columns), RowCount: rowCount,
		CanonicalSizeBytes: canonicalSize, SourceSha256: slices.Clone(sourceDigest[:]), ContentSha256: slices.Clone(contentDigest[:]),
		CreatedAt: timestamppb.New(created), UpdatedAt: timestamppb.New(updated),
	}
	if !disabled.IsZero() {
		result.DisabledAt = timestamppb.New(disabled)
	}
	if !deleted.IsZero() {
		result.DeletedAt = timestamppb.New(deleted)
	}
	return result
}

func stateTimes(value string, _ time.Time, disabled, deleted time.Time) (opensplunkv1.LookupState, time.Time, time.Time, error) {
	switch value {
	case "ACTIVE":
		if !disabled.IsZero() || !deleted.IsZero() {
			return 0, time.Time{}, time.Time{}, ErrCorrupt
		}
		return opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE, time.Time{}, time.Time{}, nil
	case "DISABLED":
		if disabled.IsZero() {
			return 0, time.Time{}, time.Time{}, ErrCorrupt
		}
		if !deleted.IsZero() {
			return 0, time.Time{}, time.Time{}, ErrCorrupt
		}
		return opensplunkv1.LookupState_LOOKUP_STATE_DISABLED, disabled, time.Time{}, nil
	case "DELETED":
		if disabled.IsZero() || deleted.IsZero() || deleted.Before(disabled) {
			return 0, time.Time{}, time.Time{}, ErrCorrupt
		}
		return opensplunkv1.LookupState_LOOKUP_STATE_DELETED, disabled, deleted, nil
	default:
		return 0, time.Time{}, time.Time{}, ErrCorrupt
	}
}

func nullTime(value sql.NullInt64) time.Time {
	if !value.Valid || value.Int64 < 1 {
		return time.Time{}
	}
	return time.UnixMicro(value.Int64).UTC()
}

func nullInt64Value(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func (catalog *Catalog) validateContext(ctx context.Context) error {
	if catalog == nil || catalog.database == nil || catalog.assets == nil || ctx == nil {
		return fmt.Errorf("%w: lookup catalog dependencies are incomplete", ErrInvalid)
	}
	return ctx.Err()
}

func (catalog *Catalog) now() (time.Time, error) {
	now := catalog.clock().UTC().Truncate(time.Microsecond)
	if now.UnixMicro() < 1 || now.UnixMicro() > maximumUnixMicro {
		return time.Time{}, fmt.Errorf("%w: lookup catalog clock is out of range", ErrInvalid)
	}
	return now, nil
}

func nextMutationTime(proposed time.Time, previousMicro int64) (time.Time, error) {
	if previousMicro < 1 || previousMicro > maximumUnixMicro {
		return time.Time{}, ErrCorrupt
	}
	proposedMicro := proposed.UnixMicro()
	if proposedMicro <= previousMicro {
		if previousMicro == maximumUnixMicro {
			return time.Time{}, fmt.Errorf("%w: lookup mutation clock cannot advance", ErrCapacity)
		}
		proposedMicro = previousMicro + 1
	}
	return time.UnixMicro(proposedMicro).UTC(), nil
}

func validIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		alphaNumeric := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
		if !alphaNumeric && (index == 0 || character != '.' && character != '_' && character != ':' && character != '-') {
			return false
		}
	}
	return true
}

func storageConflict(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint") ||
		strings.Contains(message, "primary key") ||
		strings.Contains(message, "lookup definition requires an active app workspace") ||
		strings.Contains(message, "lookup definition update requires an active app workspace")
}

func storageCapacity(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "lookup definition tenant capacity") ||
		strings.Contains(message, "lookup definition version capacity") ||
		strings.Contains(message, "lookup definition ordinary version capacity")
}

func cloneResolved(values []Resolved) []Resolved {
	result := make([]Resolved, len(values))
	for index, value := range values {
		result[index] = Resolved{Lookup: proto.Clone(value.Lookup).(*opensplunkv1.Lookup), Asset: value.Asset}
	}
	return result
}
