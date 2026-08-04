package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"gorm.io/gorm"
)

const (
	maximumAppSlugBytes           = 128
	maximumAppDisplayNameBytes    = 255
	maximumAppDescriptionBytes    = 16_384
	maximumAppDefaultIndexes      = 128
	maximumAppsPerTenant          = 256
	minimumAppCursorKeyBytes      = 32
	maximumAppCursorKeyBytes      = 1_024
	maximumAppIDAttempts          = 4
	canonicalAppIDBytes           = 26
	maximumAppTimeExpressionBytes = 1_024
)

var (
	appSlugPattern                         = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	appIDPattern                           = regexp.MustCompile(`^app_[A-Za-z0-9_-]{21}[AQgw]$`)
	errAppIDCollision                      = errors.New("app ID collision")
	errPersistedAppDefaultIndexUnavailable = errors.New(
		"app default index is unavailable in control-plane database",
	)
)

// AppAccessScope is the authenticated tenant boundary for app administration.
// It must be derived from trusted server identity, never from a request body.
type AppAccessScope struct {
	TenantID string
}

// AppSelector selects exactly one app by stable global ID or immutable,
// tenant-local slug.
type AppSelector struct {
	AppID string
	Slug  string
}

// AppState is the reversible lifecycle state of an app workspace.
type AppState string

const (
	AppStateActive   AppState = "active"
	AppStateArchived AppState = "archived"
)

// AppTimeRange preserves authored reusable time-range field presence. Nil
// fields inherit a higher-level server default; values are never resolved to
// timestamps while being persisted.
type AppTimeRange struct {
	Earliest *string
	Latest   *string
	Timezone *string
}

// AppDefinition is the mutable app configuration except for Slug, whose
// canonical value is immutable after creation. Empty and whitespace-only
// descriptions share the canonical empty-string representation; a transport
// adapter should project that value as an absent optional description.
type AppDefinition struct {
	Slug             string
	DisplayName      string
	Description      string
	DefaultIndexes   []string
	DefaultTimeRange *AppTimeRange
}

// AppWorkspace is one detached optimistic-versioned app record.
type AppWorkspace struct {
	ID         string
	Version    uint64
	Definition AppDefinition
	State      AppState
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ArchivedAt *time.Time
}

// AppCatalogOptions contains process dependencies. CursorKey must be a stable
// secret so list tokens survive restarts and cannot be forged. Clock and
// IDGenerator have safe production defaults and are injectable for tests.
type AppCatalogOptions struct {
	CursorKey   []byte
	Clock       func() time.Time
	IDGenerator func() (string, error)
}

// AppCatalog owns tenant-scoped GORM persistence over an already configured
// control DB. SQL migrations remain the only schema authority.
type AppCatalog struct {
	orm         *gorm.DB
	clock       func() time.Time
	idGenerator func() (string, error)
	cursorKey   []byte
}

// NewAppCatalog constructs an app catalog without changing the SQL schema.
func NewAppCatalog(db *DB, options AppCatalogOptions) (*AppCatalog, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", ErrInvalidArgument)
	}
	if len(options.CursorKey) < minimumAppCursorKeyBytes {
		return nil, fmt.Errorf(
			"%w: app cursor key must contain at least %d bytes",
			ErrInvalidArgument,
			minimumAppCursorKeyBytes,
		)
	}
	if len(options.CursorKey) > maximumAppCursorKeyBytes {
		return nil, fmt.Errorf(
			"%w: app cursor key exceeds the supported maximum",
			ErrInvalidArgument,
		)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = func() (string, error) {
			return randomID("app_", 16)
		}
	}
	return &AppCatalog{
		orm:         db.GORMDB(),
		clock:       clock,
		idGenerator: idGenerator,
		cursorKey:   slices.Clone(options.CursorKey),
	}, nil
}

// CreateApp creates an active app at version one. The app row and every
// default-index membership are committed atomically under SQLite's IMMEDIATE
// writer transaction.
func (catalog *AppCatalog) CreateApp(
	ctx context.Context,
	scope AppAccessScope,
	definition AppDefinition,
) (AppWorkspace, error) {
	return catalog.createApp(ctx, scope, definition, nil)
}

func (catalog *AppCatalog) createApp(
	ctx context.Context,
	scope AppAccessScope,
	definition AppDefinition,
	auditPublisher appMutationAuditPublisher,
) (AppWorkspace, error) {
	if err := validateAppContext(ctx); err != nil {
		return AppWorkspace{}, err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return AppWorkspace{}, err
	}
	normalized, err := normalizeAppDefinition(definition)
	if err != nil {
		return AppWorkspace{}, err
	}
	now, err := normalizeAppClock(catalog.clock())
	if err != nil {
		return AppWorkspace{}, err
	}

	for attempt := 0; attempt < maximumAppIDAttempts; attempt++ {
		appID, generateErr := catalog.idGenerator()
		if generateErr != nil {
			return AppWorkspace{}, fmt.Errorf("generate app ID: %w", generateErr)
		}
		if !validCanonicalAppID(appID) {
			return AppWorkspace{}, errors.New("generate app ID: generator returned an invalid ID")
		}
		created, createErr := catalog.createAppOnce(
			ctx,
			tenantID,
			appID,
			normalized,
			now,
			auditPublisher,
		)
		if errors.Is(createErr, errAppIDCollision) {
			continue
		}
		return created, createErr
	}
	return AppWorkspace{}, errors.New("create app: repeated random ID collision")
}

func (catalog *AppCatalog) createAppOnce(
	ctx context.Context,
	tenantID string,
	appID string,
	definition AppDefinition,
	now time.Time,
	auditPublisher appMutationAuditPublisher,
) (result AppWorkspace, returnedErr error) {
	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "begin app creation", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)

	var existing appRecord
	slugErr := tx.Select("app_id").
		Where("tenant_id = ? AND slug = ?", tenantID, definition.Slug).
		Take(&existing).Error
	switch {
	case slugErr == nil:
		return AppWorkspace{}, ErrAlreadyExists
	case !errors.Is(slugErr, gorm.ErrRecordNotFound):
		return AppWorkspace{}, appContextError(ctx, "check app slug", slugErr)
	}
	var appCount int64
	if err := tx.Model(&appRecord{}).Where("tenant_id = ?", tenantID).Count(&appCount).Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "count tenant apps", err)
	}
	if appCount >= maximumAppsPerTenant {
		return AppWorkspace{}, ErrCapacityExceeded
	}
	idErr := tx.Select("app_id").Where("app_id = ?", appID).Take(&existing).Error
	switch {
	case idErr == nil:
		return AppWorkspace{}, errAppIDCollision
	case !errors.Is(idErr, gorm.ErrRecordNotFound):
		return AppWorkspace{}, appContextError(ctx, "check app ID", idErr)
	}
	var legacyReference string
	referenceErr := tx.Table("saved_searches").
		Select("saved_search_id").
		Where("app_id = ?", appID).
		Limit(1).
		Scan(&legacyReference).Error
	if referenceErr != nil {
		return AppWorkspace{}, appContextError(ctx, "check app ID namespace", referenceErr)
	}
	if legacyReference != "" {
		return AppWorkspace{}, errAppIDCollision
	}

	memberships, err := resolveAppDefaultIndexes(tx, tenantID, appID, definition.DefaultIndexes)
	if err != nil {
		return AppWorkspace{}, err
	}
	record := newAppRecord(appID, tenantID, definition, now)
	if err := tx.Create(&record).Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "create app", err)
	}
	if len(memberships) != 0 {
		if err := tx.Create(&memberships).Error; err != nil {
			return AppWorkspace{}, appContextError(ctx, "create app default indexes", err)
		}
	}
	result, err = appFromRecord(record, definition.DefaultIndexes)
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("read created app: %w", err)
	}
	if err := publishAppMutationAudit(
		ctx,
		tx,
		tenantID,
		auditPublisher,
		AppMutationAuditEvent{
			OccurredAt: result.CreatedAt,
			Action:     AppMutationAuditActionCreate,
			AppID:      result.ID,
			AppVersion: result.Version,
		},
	); err != nil {
		return AppWorkspace{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "commit app creation", err)
	}
	return result, nil
}

// GetApp reads one tenant-scoped app by ID or slug without disclosing whether
// another tenant owns the selector.
func (catalog *AppCatalog) GetApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
) (result AppWorkspace, returnedErr error) {
	if err := validateAppContext(ctx); err != nil {
		return AppWorkspace{}, err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return AppWorkspace{}, err
	}
	normalizedSelector, err := normalizeAppSelector(selector)
	if err != nil {
		return AppWorkspace{}, err
	}

	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "begin app read", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	record, err := takeAppRecord(tx, tenantID, normalizedSelector)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return AppWorkspace{}, ErrNotFound
	}
	if err != nil {
		return AppWorkspace{}, appContextError(ctx, "get app", err)
	}
	namesByApp, err := loadAppDefaultIndexNames(
		tx,
		tenantID,
		[]appRecord{record},
		false,
	)
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("get app: %w", err)
	}
	result, err = appFromRecord(record, namesByApp[record.AppID])
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("get app: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "commit app read", err)
	}
	return result, nil
}

// UpdateApp replaces the complete definition under optimistic locking. Slug
// remains immutable and every requested default index is revalidated.
func (catalog *AppCatalog) UpdateApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	definition AppDefinition,
) (result AppWorkspace, returnedErr error) {
	return catalog.updateApp(
		ctx,
		scope,
		selector,
		expectedVersion,
		definition,
		nil,
	)
}

func (catalog *AppCatalog) updateApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	definition AppDefinition,
	auditPublisher appMutationAuditPublisher,
) (result AppWorkspace, returnedErr error) {
	if err := validateAppContext(ctx); err != nil {
		return AppWorkspace{}, err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return AppWorkspace{}, err
	}
	normalizedSelector, err := normalizeAppSelector(selector)
	if err != nil {
		return AppWorkspace{}, err
	}
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return AppWorkspace{}, err
	}
	normalized, err := normalizeAppDefinition(definition)
	if err != nil {
		return AppWorkspace{}, err
	}

	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "begin app update", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	current, err := takeAppRecord(tx, tenantID, normalizedSelector)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return AppWorkspace{}, ErrNotFound
	}
	if err != nil {
		return AppWorkspace{}, appContextError(ctx, "read app for update", err)
	}
	currentWorkspace, err := appFromRecord(current, nil)
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("read app for update: %w", err)
	}
	if currentWorkspace.Version != expectedVersion {
		return AppWorkspace{}, ErrVersionConflict
	}
	if currentWorkspace.Definition.Slug != normalized.Slug {
		return AppWorkspace{}, ErrImmutableSlug
	}
	memberships, err := resolveAppDefaultIndexes(tx, tenantID, current.AppID, normalized.DefaultIndexes)
	if err != nil {
		return AppWorkspace{}, err
	}
	now, err := nextAppUpdateTime(catalog.clock(), currentWorkspace.UpdatedAt)
	if err != nil {
		return AppWorkspace{}, err
	}
	updates := appDefinitionUpdates(normalized, now)
	update := tx.Model(&appRecord{}).
		Where(
			"tenant_id = ? AND app_id = ? AND version = ?",
			tenantID,
			current.AppID,
			int64(expectedVersion), // #nosec G115 -- validateExpectedVersion bounds this value.
		).
		Updates(updates)
	if update.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "update app", update.Error)
	}
	if update.RowsAffected != 1 {
		return AppWorkspace{}, ErrVersionConflict
	}
	if err := tx.Where("tenant_id = ? AND app_id = ?", tenantID, current.AppID).
		Delete(&appDefaultIndexRecord{}).Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "replace app default indexes", err)
	}
	if len(memberships) != 0 {
		if err := tx.Create(&memberships).Error; err != nil {
			return AppWorkspace{}, appContextError(ctx, "replace app default indexes", err)
		}
	}
	updated, err := takeAppRecord(tx, tenantID, AppSelector{AppID: current.AppID})
	if err != nil {
		return AppWorkspace{}, appContextError(ctx, "read updated app", err)
	}
	result, err = appFromRecord(updated, normalized.DefaultIndexes)
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("read updated app: %w", err)
	}
	if err := publishAppMutationAudit(
		ctx,
		tx,
		tenantID,
		auditPublisher,
		AppMutationAuditEvent{
			OccurredAt: result.UpdatedAt,
			Action:     AppMutationAuditActionUpdate,
			AppID:      result.ID,
			AppVersion: result.Version,
		},
	); err != nil {
		return AppWorkspace{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "commit app update", err)
	}
	return result, nil
}

// SetAppState archives or reactivates an app under optimistic locking.
// Reactivation revalidates every default index as active and searchable.
func (catalog *AppCatalog) SetAppState(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	state AppState,
) (result AppWorkspace, returnedErr error) {
	return catalog.setAppState(ctx, scope, selector, expectedVersion, state, nil)
}

func (catalog *AppCatalog) setAppState(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	state AppState,
	auditPublisher appMutationAuditPublisher,
) (result AppWorkspace, returnedErr error) {
	if err := validateAppContext(ctx); err != nil {
		return AppWorkspace{}, err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return AppWorkspace{}, err
	}
	normalizedSelector, err := normalizeAppSelector(selector)
	if err != nil {
		return AppWorkspace{}, err
	}
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return AppWorkspace{}, err
	}
	if !validAppState(state) {
		return AppWorkspace{}, fmt.Errorf("%w: unknown app state", ErrInvalidArgument)
	}

	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "begin app state update", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	current, err := takeAppRecord(tx, tenantID, normalizedSelector)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return AppWorkspace{}, ErrNotFound
	}
	if err != nil {
		return AppWorkspace{}, appContextError(ctx, "read app for state update", err)
	}
	currentWorkspace, err := appFromRecord(current, nil)
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("read app for state update: %w", err)
	}
	if currentWorkspace.Version != expectedVersion {
		return AppWorkspace{}, ErrVersionConflict
	}
	namesByApp, err := loadAppDefaultIndexNames(
		tx,
		tenantID,
		[]appRecord{current},
		state == AppStateActive,
	)
	if err != nil {
		if errors.Is(err, errPersistedAppDefaultIndexUnavailable) &&
			state == AppStateActive {
			return AppWorkspace{}, ErrDependencyConflict
		}
		return AppWorkspace{}, fmt.Errorf("read app default indexes for state update: %w", err)
	}
	now, err := nextAppUpdateTime(catalog.clock(), currentWorkspace.UpdatedAt)
	if err != nil {
		return AppWorkspace{}, err
	}
	var archivedAt *int64
	if state == AppStateArchived {
		value := now.UnixMicro()
		archivedAt = &value
	}
	update := tx.Model(&appRecord{}).
		Where(
			"tenant_id = ? AND app_id = ? AND version = ?",
			tenantID,
			current.AppID,
			int64(expectedVersion), // #nosec G115 -- validateExpectedVersion bounds this value.
		).
		Updates(map[string]any{
			"archived_at_unix_micro": archivedAt,
			"state":                  state,
			"updated_at_unix_micro":  now.UnixMicro(),
			"version":                gorm.Expr("version + 1"),
		})
	if update.Error != nil {
		return AppWorkspace{}, appContextError(ctx, "set app state", update.Error)
	}
	if update.RowsAffected != 1 {
		return AppWorkspace{}, ErrVersionConflict
	}
	updated, err := takeAppRecord(tx, tenantID, AppSelector{AppID: current.AppID})
	if err != nil {
		return AppWorkspace{}, appContextError(ctx, "read app after state update", err)
	}
	result, err = appFromRecord(updated, namesByApp[current.AppID])
	if err != nil {
		return AppWorkspace{}, fmt.Errorf("read app after state update: %w", err)
	}
	action := AppMutationAuditActionActivate
	if result.State == AppStateArchived {
		action = AppMutationAuditActionArchive
	}
	if err := publishAppMutationAudit(
		ctx,
		tx,
		tenantID,
		auditPublisher,
		AppMutationAuditEvent{
			OccurredAt: result.UpdatedAt,
			Action:     action,
			AppID:      result.ID,
			AppVersion: result.Version,
		},
	); err != nil {
		return AppWorkspace{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return AppWorkspace{}, appContextError(ctx, "commit app state update", err)
	}
	return result, nil
}

// DeleteApp permanently deletes only an archived, unreferenced app under
// optimistic locking and an exact canonical slug confirmation. The IMMEDIATE
// transaction and SQL triggers make the dependency check race-safe.
func (catalog *AppCatalog) DeleteApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	confirmationSlug string,
) (deletedID string, returnedErr error) {
	return catalog.deleteApp(
		ctx,
		scope,
		selector,
		expectedVersion,
		confirmationSlug,
		nil,
	)
}

func (catalog *AppCatalog) deleteApp(
	ctx context.Context,
	scope AppAccessScope,
	selector AppSelector,
	expectedVersion uint64,
	confirmationSlug string,
	auditPublisher appMutationAuditPublisher,
) (deletedID string, returnedErr error) {
	if err := validateAppContext(ctx); err != nil {
		return "", err
	}
	tenantID, err := normalizeAppScope(scope)
	if err != nil {
		return "", err
	}
	normalizedSelector, err := normalizeAppSelector(selector)
	if err != nil {
		return "", err
	}
	if err := validateExpectedVersion(expectedVersion); err != nil {
		return "", err
	}
	normalizedConfirmation, err := NormalizeAppSlug(confirmationSlug)
	if err != nil || normalizedConfirmation != confirmationSlug {
		return "", fmt.Errorf("%w: app deletion confirmation is invalid", ErrInvalidArgument)
	}

	tx := catalog.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return "", appContextError(ctx, "begin app deletion", tx.Error)
	}
	defer finishGORMTx(tx, &returnedErr)
	current, err := takeAppRecord(tx, tenantID, normalizedSelector)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", appContextError(ctx, "read app for deletion", err)
	}
	currentWorkspace, err := appFromRecord(current, nil)
	if err != nil {
		return "", fmt.Errorf("read app for deletion: %w", err)
	}
	if currentWorkspace.Version != expectedVersion {
		return "", ErrVersionConflict
	}
	if currentWorkspace.Definition.Slug != normalizedConfirmation {
		return "", fmt.Errorf("%w: app deletion confirmation does not match", ErrInvalidArgument)
	}
	if currentWorkspace.State != AppStateArchived {
		return "", ErrDependencyConflict
	}
	var dependencies int64
	if err := tx.Table("saved_searches").
		Where("app_id = ?", current.AppID).
		Count(&dependencies).Error; err != nil {
		return "", appContextError(ctx, "check app dependencies", err)
	}
	if dependencies != 0 {
		return "", ErrDependencyConflict
	}
	deleted := tx.Where(
		"tenant_id = ? AND app_id = ? AND version = ? AND state = ?",
		tenantID,
		current.AppID,
		int64(expectedVersion), // #nosec G115 -- validateExpectedVersion bounds this value.
		AppStateArchived,
	).Delete(&appRecord{})
	if deleted.Error != nil {
		return "", appContextError(ctx, "delete app", deleted.Error)
	}
	if deleted.RowsAffected != 1 {
		return "", ErrVersionConflict
	}
	if auditPublisher != nil {
		occurredAt, clockErr := nextAppUpdateTime(
			catalog.clock(),
			currentWorkspace.UpdatedAt,
		)
		if clockErr != nil {
			return "", clockErr
		}
		if err := publishAppMutationAudit(
			ctx,
			tx,
			tenantID,
			auditPublisher,
			AppMutationAuditEvent{
				OccurredAt: occurredAt,
				Action:     AppMutationAuditActionDelete,
				AppID:      currentWorkspace.ID,
				AppVersion: currentWorkspace.Version,
			},
		); err != nil {
			return "", err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return "", appContextError(ctx, "commit app deletion", err)
	}
	return current.AppID, nil
}

func takeAppRecord(database *gorm.DB, tenantID string, selector AppSelector) (appRecord, error) {
	var record appRecord
	query := database.Where("tenant_id = ?", tenantID)
	if selector.AppID != "" {
		query = query.Where("app_id = ?", selector.AppID)
	} else {
		query = query.Where("slug = ?", selector.Slug)
	}
	result := query.Take(&record)
	return record, result.Error
}

func newAppRecord(
	appID string,
	tenantID string,
	definition AppDefinition,
	now time.Time,
) appRecord {
	present, earliest, latest, timezone := appTimeRangeColumns(definition.DefaultTimeRange)
	return appRecord{
		AppID:                   appID,
		TenantID:                tenantID,
		Version:                 1,
		Slug:                    definition.Slug,
		DisplayName:             definition.DisplayName,
		Description:             definition.Description,
		DefaultTimeRangePresent: present,
		DefaultEarliest:         earliest,
		DefaultLatest:           latest,
		DefaultTimezone:         timezone,
		State:                   AppStateActive,
		CreatedAtUnixMicro:      now.UnixMicro(),
		UpdatedAtUnixMicro:      now.UnixMicro(),
	}
}

func appDefinitionUpdates(definition AppDefinition, now time.Time) map[string]any {
	present, earliest, latest, timezone := appTimeRangeColumns(definition.DefaultTimeRange)
	return map[string]any{
		"default_earliest":           earliest,
		"default_latest":             latest,
		"default_time_range_present": present,
		"default_timezone":           timezone,
		"description":                definition.Description,
		"display_name":               definition.DisplayName,
		"updated_at_unix_micro":      now.UnixMicro(),
		"version":                    gorm.Expr("version + 1"),
	}
}

func appFromRecord(record appRecord, defaultIndexes []string) (AppWorkspace, error) {
	if !validCanonicalAppID(record.AppID) ||
		validateTenantID(record.TenantID) != nil ||
		record.Version < 1 ||
		record.DefaultTimeRangePresent < 0 ||
		record.DefaultTimeRangePresent > 1 ||
		!validAppState(record.State) ||
		record.CreatedAtUnixMicro <= 0 ||
		record.UpdatedAtUnixMicro < record.CreatedAtUnixMicro {
		return AppWorkspace{}, invalidAppRecordError()
	}
	slug, err := NormalizeAppSlug(record.Slug)
	if err != nil || slug != record.Slug {
		return AppWorkspace{}, invalidAppRecordError()
	}
	if err := validateCanonicalAppText(record.DisplayName, maximumAppDisplayNameBytes, false); err != nil {
		return AppWorkspace{}, invalidAppRecordError()
	}
	if err := validateCanonicalAppText(record.Description, maximumAppDescriptionBytes, true); err != nil {
		return AppWorkspace{}, invalidAppRecordError()
	}
	timeRange, err := appTimeRangeFromRecord(record)
	if err != nil {
		return AppWorkspace{}, invalidAppRecordError()
	}
	if len(defaultIndexes) > maximumAppDefaultIndexes || !slices.IsSorted(defaultIndexes) {
		return AppWorkspace{}, invalidAppRecordError()
	}
	for index, name := range defaultIndexes {
		normalized, normalizeErr := NormalizeIndexName(name)
		if normalizeErr != nil || normalized != name ||
			index > 0 && defaultIndexes[index-1] == name {
			return AppWorkspace{}, invalidAppRecordError()
		}
	}
	createdAt := time.UnixMicro(record.CreatedAtUnixMicro).UTC()
	updatedAt := time.UnixMicro(record.UpdatedAtUnixMicro).UTC()
	var archivedAt *time.Time
	switch record.State {
	case AppStateActive:
		if record.ArchivedAtUnixMicro != nil {
			return AppWorkspace{}, invalidAppRecordError()
		}
	case AppStateArchived:
		if record.ArchivedAtUnixMicro == nil ||
			*record.ArchivedAtUnixMicro < record.CreatedAtUnixMicro ||
			*record.ArchivedAtUnixMicro > record.UpdatedAtUnixMicro {
			return AppWorkspace{}, invalidAppRecordError()
		}
		value := time.UnixMicro(*record.ArchivedAtUnixMicro).UTC()
		archivedAt = &value
	}
	return AppWorkspace{
		ID:      record.AppID,
		Version: uint64(record.Version),
		Definition: AppDefinition{
			Slug:             record.Slug,
			DisplayName:      record.DisplayName,
			Description:      record.Description,
			DefaultIndexes:   slices.Clone(defaultIndexes),
			DefaultTimeRange: cloneAppTimeRange(timeRange),
		},
		State:      record.State,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
		ArchivedAt: archivedAt,
	}, nil
}

func normalizeAppDefinition(input AppDefinition) (AppDefinition, error) {
	slug, err := NormalizeAppSlug(input.Slug)
	if err != nil {
		return AppDefinition{}, err
	}
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		displayName = slug
	}
	if err := validateCanonicalAppText(displayName, maximumAppDisplayNameBytes, false); err != nil {
		return AppDefinition{}, err
	}
	description := strings.TrimSpace(input.Description)
	if err := validateCanonicalAppText(description, maximumAppDescriptionBytes, true); err != nil {
		return AppDefinition{}, err
	}
	if len(input.DefaultIndexes) > maximumAppDefaultIndexes {
		return AppDefinition{}, fmt.Errorf(
			"%w: app default indexes exceed the supported count",
			ErrInvalidArgument,
		)
	}
	defaultIndexes := make([]string, 0, len(input.DefaultIndexes))
	for _, name := range input.DefaultIndexes {
		normalized, normalizeErr := NormalizeIndexName(name)
		if normalizeErr != nil {
			return AppDefinition{}, fmt.Errorf("%w: invalid app default index", ErrInvalidArgument)
		}
		defaultIndexes = append(defaultIndexes, normalized)
	}
	slices.Sort(defaultIndexes)
	defaultIndexes = slices.Compact(defaultIndexes)
	timeRange, err := normalizeAppTimeRange(input.DefaultTimeRange)
	if err != nil {
		return AppDefinition{}, err
	}
	return AppDefinition{
		Slug:             slug,
		DisplayName:      displayName,
		Description:      description,
		DefaultIndexes:   defaultIndexes,
		DefaultTimeRange: timeRange,
	}, nil
}

// NormalizeAppSlug canonicalizes an immutable tenant-local app slug.
func NormalizeAppSlug(input string) (string, error) {
	slug := strings.ToLower(strings.TrimSpace(input))
	if len(slug) == 0 || len(slug) > maximumAppSlugBytes ||
		!appSlugPattern.MatchString(slug) {
		return "", fmt.Errorf("%w: app slug is invalid", ErrInvalidArgument)
	}
	return slug, nil
}

func normalizeAppTimeRange(input *AppTimeRange) (*AppTimeRange, error) {
	if input == nil {
		return nil, nil
	}
	result := &AppTimeRange{}
	var err error
	result.Earliest, err = normalizeOptionalAppText(
		input.Earliest,
		maximumAppTimeExpressionBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: default earliest expression is invalid", ErrInvalidArgument)
	}
	result.Latest, err = normalizeOptionalAppText(
		input.Latest,
		maximumAppTimeExpressionBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: default latest expression is invalid", ErrInvalidArgument)
	}
	result.Timezone, err = normalizeOptionalAppText(
		input.Timezone,
		ianatimezone.MaximumNameBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: default timezone is invalid", ErrInvalidArgument)
	}
	effectiveEarliest := "-24h"
	effectiveLatest := "now"
	effectiveTimezone := "UTC"
	if result.Earliest != nil {
		effectiveEarliest = *result.Earliest
	}
	if result.Latest != nil {
		effectiveLatest = *result.Latest
	}
	if result.Timezone != nil {
		effectiveTimezone = *result.Timezone
	}
	if !validAppTimeExpression(effectiveEarliest, true) ||
		!validAppTimeExpression(effectiveLatest, false) {
		return nil, fmt.Errorf("%w: app default time range is invalid", ErrInvalidArgument)
	}
	if _, err := ianatimezone.Load(effectiveTimezone); err != nil {
		return nil, fmt.Errorf("%w: app default time range is invalid", ErrInvalidArgument)
	}
	return result, nil
}

// validAppTimeExpression mirrors the pure reusable-intent syntax accepted by
// searchtime.ValidateIntent. control cannot import searchtime directly because
// the execution/storage dependency graph returns to control through
// visibility. This validator deliberately never resolves against wall time.
func validAppTimeExpression(expression string, earliest bool) bool {
	if expression == "now" {
		return true
	}
	if expression == "0" {
		return earliest
	}
	if expression == "@d" {
		return true
	}
	if strings.HasPrefix(expression, "-") && strings.HasSuffix(expression, "d@d") {
		digits := expression[1 : len(expression)-len("d@d")]
		amount, ok := parseAppDecimal(digits)
		return ok && amount > 0 && amount <= searchtimebounds.MaximumSpanDays
	}
	if len(expression) >= 3 && expression[0] == '-' {
		unit := expression[len(expression)-1]
		if unit == 's' || unit == 'm' || unit == 'h' || unit == 'd' {
			amount, ok := parseAppDecimal(expression[1 : len(expression)-1])
			if !ok || amount == 0 {
				return false
			}
			switch unit {
			case 'd':
				return amount <= math.MaxInt32
			case 'h':
				return amount <= uint64(math.MaxInt64)/(60*60)
			case 'm':
				return amount <= uint64(math.MaxInt64)/60
			default:
				return amount <= uint64(math.MaxInt64)
			}
		}
	}
	if !validAppRFC3339Nano(expression) {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, expression)
	return err == nil
}

func parseAppDecimal(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func validAppRFC3339Nano(value string) bool {
	if len(value) < len("0001-01-01T00:00:00Z") ||
		value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
		value[13] != ':' || value[16] != ':' {
		return false
	}
	for _, index := range [...]int{0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18} {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	zoneStart := 19
	if value[zoneStart] == '.' {
		fractionStart := zoneStart + 1
		zoneStart = fractionStart
		for zoneStart < len(value) && value[zoneStart] >= '0' && value[zoneStart] <= '9' {
			zoneStart++
		}
		fractionDigits := zoneStart - fractionStart
		if fractionDigits < 1 || fractionDigits > 9 {
			return false
		}
	}
	if zoneStart == len(value)-1 && value[zoneStart] == 'Z' {
		return true
	}
	if len(value)-zoneStart != len("+00:00") ||
		(value[zoneStart] != '+' && value[zoneStart] != '-') ||
		value[zoneStart+3] != ':' {
		return false
	}
	for _, index := range [...]int{zoneStart + 1, zoneStart + 2, zoneStart + 4, zoneStart + 5} {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	hour := int(value[zoneStart+1]-'0')*10 + int(value[zoneStart+2]-'0')
	minute := int(value[zoneStart+4]-'0')*10 + int(value[zoneStart+5]-'0')
	return hour <= 23 && minute <= 59
}

func normalizeOptionalAppText(value *string, maximum int) (*string, error) {
	if value == nil {
		return nil, nil
	}
	if len(*value) > maximum || !utf8.ValidString(*value) {
		return nil, ErrInvalidArgument
	}
	normalized := strings.TrimSpace(*value)
	if normalized == "" || len(normalized) > maximum ||
		strings.IndexByte(normalized, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	for _, character := range normalized {
		if unicode.IsControl(character) {
			return nil, ErrInvalidArgument
		}
	}
	return &normalized, nil
}

func appTimeRangeColumns(value *AppTimeRange) (int64, *string, *string, *string) {
	if value == nil {
		return 0, nil, nil, nil
	}
	return 1, cloneString(value.Earliest), cloneString(value.Latest), cloneString(value.Timezone)
}

func appTimeRangeFromRecord(record appRecord) (*AppTimeRange, error) {
	if record.DefaultTimeRangePresent == 0 {
		if record.DefaultEarliest != nil || record.DefaultLatest != nil || record.DefaultTimezone != nil {
			return nil, invalidAppRecordError()
		}
		return nil, nil
	}
	timeRange, err := normalizeAppTimeRange(&AppTimeRange{
		Earliest: record.DefaultEarliest,
		Latest:   record.DefaultLatest,
		Timezone: record.DefaultTimezone,
	})
	if err != nil ||
		!equalOptionalString(timeRange.Earliest, record.DefaultEarliest) ||
		!equalOptionalString(timeRange.Latest, record.DefaultLatest) ||
		!equalOptionalString(timeRange.Timezone, record.DefaultTimezone) {
		return nil, invalidAppRecordError()
	}
	return timeRange, nil
}

func resolveAppDefaultIndexes(
	database *gorm.DB,
	tenantID string,
	appID string,
	names []string,
) ([]appDefaultIndexRecord, error) {
	if len(names) == 0 {
		return nil, nil
	}
	var records []indexRecord
	if err := database.Where("name IN ?", names).Find(&records).Error; err != nil {
		return nil, fmt.Errorf("resolve app default indexes: %w", err)
	}
	if len(records) != len(names) {
		return nil, fmt.Errorf("%w: app default index is unavailable", ErrInvalidArgument)
	}
	byName := make(map[string]indexRecord, len(records))
	for _, record := range records {
		index, err := indexFromRecord(record)
		if err != nil {
			return nil, errors.New("resolve app default indexes: invalid index record in control-plane database")
		}
		if index.State != IndexStateActive || !index.Definition.SearchEnabled {
			return nil, fmt.Errorf("%w: app default index is unavailable", ErrInvalidArgument)
		}
		byName[index.Definition.Name] = record
	}
	memberships := make([]appDefaultIndexRecord, 0, len(names))
	for _, name := range names {
		record, exists := byName[name]
		if !exists {
			return nil, fmt.Errorf("%w: app default index is unavailable", ErrInvalidArgument)
		}
		memberships = append(memberships, appDefaultIndexRecord{
			TenantID: tenantID,
			AppID:    appID,
			IndexID:  record.IndexID,
		})
	}
	return memberships, nil
}

type appDefaultIndexJoin struct {
	AppID         string         `gorm:"column:app_id"`
	IndexID       string         `gorm:"column:index_id"`
	IndexName     sql.NullString `gorm:"column:index_name"`
	SearchEnabled sql.NullInt64  `gorm:"column:search_enabled"`
	IndexState    sql.NullString `gorm:"column:index_state"`
}

func loadAppDefaultIndexNames(
	database *gorm.DB,
	tenantID string,
	apps []appRecord,
	requireSearchableAll bool,
) (map[string][]string, error) {
	result := make(map[string][]string, len(apps))
	states := make(map[string]AppState, len(apps))
	ids := make([]string, 0, len(apps))
	for _, app := range apps {
		result[app.AppID] = []string{}
		states[app.AppID] = app.State
		ids = append(ids, app.AppID)
	}
	if len(ids) == 0 {
		return result, nil
	}
	var rows []appDefaultIndexJoin
	query := database.Table("app_default_indexes AS app_index").
		Select(`
			app_index.app_id AS app_id,
			app_index.index_id AS index_id,
			search_index.name AS index_name,
			search_index.search_enabled AS search_enabled,
			search_index.state AS index_state`).
		Joins("LEFT JOIN indexes AS search_index ON search_index.index_id = app_index.index_id").
		Where("app_index.tenant_id = ? AND app_index.app_id IN ?", tenantID, ids).
		Order("app_index.app_id ASC, search_index.name ASC, app_index.index_id ASC").
		Scan(&rows)
	if query.Error != nil {
		return nil, fmt.Errorf("read app default indexes: %w", query.Error)
	}
	for _, row := range rows {
		state, requested := states[row.AppID]
		if !requested ||
			!row.IndexName.Valid ||
			!row.SearchEnabled.Valid ||
			!row.IndexState.Valid ||
			(row.SearchEnabled.Int64 != 0 && row.SearchEnabled.Int64 != 1) ||
			!validIndexState(IndexState(row.IndexState.String)) {
			return nil, errors.New("invalid app default-index record in control-plane database")
		}
		name, err := NormalizeIndexName(row.IndexName.String)
		if err != nil || name != row.IndexName.String {
			return nil, errors.New("invalid app default-index record in control-plane database")
		}
		requireSearchable := requireSearchableAll || state == AppStateActive
		if requireSearchable &&
			(row.SearchEnabled.Int64 != 1 || IndexState(row.IndexState.String) != IndexStateActive) {
			return nil, errPersistedAppDefaultIndexUnavailable
		}
		names := result[row.AppID]
		if len(names) >= maximumAppDefaultIndexes ||
			len(names) != 0 && names[len(names)-1] >= name {
			return nil, errors.New("invalid app default-index record in control-plane database")
		}
		result[row.AppID] = append(names, name)
	}
	return result, nil
}

func normalizeAppScope(scope AppAccessScope) (string, error) {
	if err := validateTenantID(scope.TenantID); err != nil {
		return "", fmt.Errorf("%w: tenant identity is invalid", ErrInvalidArgument)
	}
	return strings.Clone(scope.TenantID), nil
}

func normalizeAppSelector(selector AppSelector) (AppSelector, error) {
	hasID := selector.AppID != ""
	hasSlug := selector.Slug != ""
	if hasID == hasSlug {
		return AppSelector{}, fmt.Errorf(
			"%w: exactly one app selector is required",
			ErrInvalidArgument,
		)
	}
	if hasID {
		if !validCanonicalAppID(selector.AppID) {
			return AppSelector{}, fmt.Errorf("%w: app ID is invalid", ErrInvalidArgument)
		}
		return AppSelector{AppID: strings.Clone(selector.AppID)}, nil
	}
	slug, err := NormalizeAppSlug(selector.Slug)
	if err != nil {
		return AppSelector{}, err
	}
	return AppSelector{Slug: slug}, nil
}

func validateCanonicalAppText(value string, maximum int, allowEmpty bool) error {
	if (!allowEmpty && value == "") || len(value) > maximum ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value ||
		strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: app text is invalid", ErrInvalidArgument)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: app text is invalid", ErrInvalidArgument)
		}
	}
	return nil
}

func validCanonicalAppID(value string) bool {
	return len(value) == canonicalAppIDBytes && appIDPattern.MatchString(value)
}

func validAppState(state AppState) bool {
	return state == AppStateActive || state == AppStateArchived
}

func validateAppContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func normalizeAppClock(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, fmt.Errorf("%w: app clock returned an invalid time", ErrInvalidArgument)
	}
	normalized := databaseTime(value)
	if normalized.UnixMicro() <= 0 {
		return time.Time{}, fmt.Errorf("%w: app clock returned an invalid time", ErrInvalidArgument)
	}
	return normalized, nil
}

func nextAppUpdateTime(value time.Time, previous time.Time) (time.Time, error) {
	normalized, err := normalizeAppClock(value)
	if err != nil {
		return time.Time{}, err
	}
	if normalized.After(previous) {
		return normalized, nil
	}
	if previous.UnixMicro() == math.MaxInt64 {
		return time.Time{}, errors.New("advance app update time: timestamp range exhausted")
	}
	return time.UnixMicro(previous.UnixMicro() + 1).UTC(), nil
}

func invalidAppRecordError() error {
	return errors.New("invalid app record in control-plane database")
}

func appContextError(ctx context.Context, operation string, err error) error {
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("%s: %w", operation, contextErr)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func cloneAppTimeRange(value *AppTimeRange) *AppTimeRange {
	if value == nil {
		return nil
	}
	return &AppTimeRange{
		Earliest: cloneString(value.Earliest),
		Latest:   cloneString(value.Latest),
		Timezone: cloneString(value.Timezone),
	}
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	result := strings.Clone(*value)
	return &result
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
