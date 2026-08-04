package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const (
	appCatalogCursorKeyPurpose        = "app-catalog-cursors"
	appAdministrationCursorKeyPurpose = "app-administration-cursors"
)

type controlAppCatalog interface {
	CreateApp(
		context.Context,
		control.AppAccessScope,
		control.AppDefinition,
	) (control.AppWorkspace, error)
	GetApp(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
	) (control.AppWorkspace, error)
	ListApps(
		context.Context,
		control.AppAccessScope,
		control.AppListRequest,
	) (control.AppListResult, error)
	UpdateApp(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		control.AppDefinition,
	) (control.AppWorkspace, error)
	SetAppState(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		control.AppState,
	) (control.AppWorkspace, error)
	DeleteApp(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		string,
	) (string, error)
}

// runtimeAppCatalog is a boundary adapter, not an owner. The GORM catalog
// borrows the process control database, so runtime shutdown continues to close
// that database exactly once through main.
type runtimeAppCatalog struct {
	catalog                controlAppCatalog
	mutationScopeValidator func(
		context.Context,
		server.AppAdministrationScope,
	) error
}

var _ server.AppAdministration = (*runtimeAppCatalog)(nil)
var _ server.AppCatalog = (*runtimeAppCatalog)(nil)

func newRuntimeAppCatalog(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
) (*runtimeAppCatalog, []byte, error) {
	catalog, administrationKey, err := newRuntimeControlAppCatalog(
		ctx,
		db,
		masterKeyPath,
	)
	if err != nil {
		return nil, nil, err
	}
	return &runtimeAppCatalog{catalog: catalog}, administrationKey, nil
}

// newRuntimeAuditedAppCatalog is the production app-administration
// constructor. Read-only catalog operations continue to use the same GORM
// store, while every mutation is made contingent on an audit append in its
// caller-owned SQLite transaction.
func newRuntimeAuditedAppCatalog(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
	appender control.AppMutationAuditAppender,
) (*runtimeAppCatalog, []byte, error) {
	catalog, administrationKey, err := newRuntimeControlAppCatalog(
		ctx,
		db,
		masterKeyPath,
	)
	if err != nil {
		return nil, nil, err
	}
	audited, err := control.NewAuditedAppCatalog(catalog, appender)
	if err != nil {
		clear(administrationKey)
		return nil, nil, fmt.Errorf("create audited app catalog: %w", err)
	}
	return &runtimeAppCatalog{
		catalog:                audited,
		mutationScopeValidator: validateRuntimeAppMutationActor,
	}, administrationKey, nil
}

func newRuntimeControlAppCatalog(
	ctx context.Context,
	db *control.DB,
	masterKeyPath string,
) (*control.AppCatalog, []byte, error) {
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open app-catalog master key: %w", err)
	}
	defer clear(masterKey)

	catalogCursorKey, administrationCursorKey, err := deriveAppCursorKeys(masterKey)
	if err != nil {
		return nil, nil, err
	}
	defer clear(catalogCursorKey)

	catalog, err := control.NewAppCatalog(
		db,
		control.AppCatalogOptions{CursorKey: catalogCursorKey},
	)
	if err != nil {
		clear(administrationCursorKey)
		return nil, nil, fmt.Errorf("create app catalog: %w", err)
	}
	return catalog, administrationCursorKey, nil
}

func deriveAppCursorKeys(masterKey []byte) ([]byte, []byte, error) {
	catalogKey, err := deriveServerKey(masterKey, appCatalogCursorKeyPurpose)
	if err != nil {
		return nil, nil, fmt.Errorf("derive app-catalog cursor key: %w", err)
	}
	administrationKey, err := deriveServerKey(
		masterKey,
		appAdministrationCursorKeyPurpose,
	)
	if err != nil {
		clear(catalogKey)
		return nil, nil, fmt.Errorf("derive app-administration cursor key: %w", err)
	}
	return catalogKey, administrationKey, nil
}

func (adapter *runtimeAppCatalog) CreateApp(
	ctx context.Context,
	scope server.AppAdministrationScope,
	definition server.AppAdministrationDefinition,
) (server.AppAdministrationWorkspace, error) {
	if err := adapter.validateMutationActor(ctx, scope); err != nil {
		return server.AppAdministrationWorkspace{}, err
	}
	result, err := adapter.catalog.CreateApp(
		ctx,
		controlAppScope(scope),
		controlAppDefinition(definition),
	)
	if err != nil {
		return server.AppAdministrationWorkspace{}, mapRuntimeAppCatalogError(err)
	}
	return serverAppWorkspace(result)
}

func (adapter *runtimeAppCatalog) GetApp(
	ctx context.Context,
	scope server.AppAdministrationScope,
	selector server.AppAdministrationSelector,
) (server.AppAdministrationWorkspace, error) {
	result, err := adapter.catalog.GetApp(
		ctx,
		controlAppScope(scope),
		controlAppSelector(selector),
	)
	if err != nil {
		return server.AppAdministrationWorkspace{}, mapRuntimeAppCatalogError(err)
	}
	return serverAppWorkspace(result)
}

func (adapter *runtimeAppCatalog) ListApps(
	ctx context.Context,
	scope server.AppAdministrationScope,
	request server.AppAdministrationListRequest,
) (server.AppAdministrationListResult, error) {
	convertedRequest, err := controlAppListRequest(request)
	if err != nil {
		return server.AppAdministrationListResult{}, err
	}
	result, err := adapter.catalog.ListApps(
		ctx,
		controlAppScope(scope),
		convertedRequest,
	)
	if err != nil {
		return server.AppAdministrationListResult{}, mapRuntimeAppCatalogError(err)
	}
	return serverAppListResult(result)
}

func (adapter *runtimeAppCatalog) UpdateApp(
	ctx context.Context,
	scope server.AppAdministrationScope,
	selector server.AppAdministrationSelector,
	expectedVersion uint64,
	definition server.AppAdministrationDefinition,
) (server.AppAdministrationWorkspace, error) {
	if err := adapter.validateMutationActor(ctx, scope); err != nil {
		return server.AppAdministrationWorkspace{}, err
	}
	result, err := adapter.catalog.UpdateApp(
		ctx,
		controlAppScope(scope),
		controlAppSelector(selector),
		expectedVersion,
		controlAppDefinition(definition),
	)
	if err != nil {
		return server.AppAdministrationWorkspace{}, mapRuntimeAppCatalogError(err)
	}
	return serverAppWorkspace(result)
}

func (adapter *runtimeAppCatalog) SetAppState(
	ctx context.Context,
	scope server.AppAdministrationScope,
	selector server.AppAdministrationSelector,
	expectedVersion uint64,
	state server.AppAdministrationState,
) (server.AppAdministrationWorkspace, error) {
	if err := adapter.validateMutationActor(ctx, scope); err != nil {
		return server.AppAdministrationWorkspace{}, err
	}
	convertedState, err := controlAppState(state)
	if err != nil {
		return server.AppAdministrationWorkspace{}, err
	}
	result, err := adapter.catalog.SetAppState(
		ctx,
		controlAppScope(scope),
		controlAppSelector(selector),
		expectedVersion,
		convertedState,
	)
	if err != nil {
		return server.AppAdministrationWorkspace{}, mapRuntimeAppCatalogError(err)
	}
	return serverAppWorkspace(result)
}

func (adapter *runtimeAppCatalog) DeleteApp(
	ctx context.Context,
	scope server.AppAdministrationScope,
	selector server.AppAdministrationSelector,
	expectedVersion uint64,
	confirmationSlug string,
) (string, error) {
	if err := adapter.validateMutationActor(ctx, scope); err != nil {
		return "", err
	}
	deletedID, err := adapter.catalog.DeleteApp(
		ctx,
		controlAppScope(scope),
		controlAppSelector(selector),
		expectedVersion,
		strings.Clone(confirmationSlug),
	)
	if err != nil {
		return "", mapRuntimeAppCatalogError(err)
	}
	return strings.Clone(deletedID), nil
}

func (adapter *runtimeAppCatalog) validateMutationActor(
	ctx context.Context,
	scope server.AppAdministrationScope,
) error {
	if adapter.mutationScopeValidator == nil {
		return nil
	}
	return adapter.mutationScopeValidator(ctx, scope)
}

func validateRuntimeAppMutationActor(
	ctx context.Context,
	scope server.AppAdministrationScope,
) error {
	actor, ok := audit.ActorFromContext(ctx)
	if !ok || actor.ID != scope.ActorID {
		return server.ErrAppAdministrationInvalidArgument
	}
	return nil
}

func (adapter *runtimeAppCatalog) ListActiveApps(
	ctx context.Context,
	tenantID string,
	maximum uint32,
) (server.AppCatalogResult, error) {
	if maximum == 0 {
		return server.AppCatalogResult{},
			server.ErrAppAdministrationInvalidArgument
	}
	result, err := adapter.catalog.ListApps(
		ctx,
		control.AppAccessScope{TenantID: strings.Clone(tenantID)},
		control.AppListRequest{
			PageSize:     maximum,
			StateFilters: []control.AppState{control.AppStateActive},
			SortBy:       control.AppSortByDisplayName,
			Direction:    control.AppSortAscending,
		},
	)
	if err != nil {
		return server.AppCatalogResult{}, mapRuntimeAppCatalogError(err)
	}
	if uint64(len(result.Apps)) > uint64(maximum) {
		return server.AppCatalogResult{}, errors.New(
			"convert active app catalog: storage page exceeds its bound",
		)
	}
	apps := make([]server.AppCatalogSummary, len(result.Apps))
	for index, app := range result.Apps {
		if app.State != control.AppStateActive {
			return server.AppCatalogResult{}, errors.New(
				"convert active app catalog: persisted app state is invalid",
			)
		}
		apps[index] = server.AppCatalogSummary{
			AppID:             strings.Clone(app.ID),
			Slug:              strings.Clone(app.Definition.Slug),
			DisplayName:       strings.Clone(app.Definition.DisplayName),
			DefaultIndexNames: slices.Clone(app.Definition.DefaultIndexes),
		}
	}
	return server.AppCatalogResult{
		Apps:     apps,
		Complete: result.NextPageToken == nil,
	}, nil
}

func controlAppScope(scope server.AppAdministrationScope) control.AppAccessScope {
	return control.AppAccessScope{TenantID: strings.Clone(scope.TenantID)}
}

func controlAppSelector(selector server.AppAdministrationSelector) control.AppSelector {
	return control.AppSelector{
		AppID: strings.Clone(selector.AppID),
		Slug:  strings.Clone(selector.Slug),
	}
}

func controlAppDefinition(
	definition server.AppAdministrationDefinition,
) control.AppDefinition {
	description := ""
	if definition.Description != nil {
		description = strings.Clone(*definition.Description)
	}
	return control.AppDefinition{
		Slug:             strings.Clone(definition.Slug),
		DisplayName:      strings.Clone(definition.DisplayName),
		Description:      description,
		DefaultIndexes:   slices.Clone(definition.DefaultIndexNames),
		DefaultTimeRange: controlAppTimeRange(definition.DefaultTimeRange),
	}
}

func controlAppTimeRange(
	value *server.AppAdministrationTimeRange,
) *control.AppTimeRange {
	if value == nil {
		return nil
	}
	return &control.AppTimeRange{
		Earliest: cloneRuntimeString(value.Earliest),
		Latest:   cloneRuntimeString(value.Latest),
		Timezone: cloneRuntimeString(value.Timezone),
	}
}

func controlAppListRequest(
	request server.AppAdministrationListRequest,
) (control.AppListRequest, error) {
	states := make([]control.AppState, len(request.StateFilters))
	for index, state := range request.StateFilters {
		converted, err := controlAppState(state)
		if err != nil {
			return control.AppListRequest{}, err
		}
		states[index] = converted
	}
	var textFilter *string
	if request.TextFilter != "" {
		textFilter = cloneRuntimeString(&request.TextFilter)
	}
	sortBy, err := controlAppSort(request.SortBy)
	if err != nil {
		return control.AppListRequest{}, err
	}
	direction := control.AppSortAscending
	if request.SortDescending {
		direction = control.AppSortDescending
	}
	return control.AppListRequest{
		PageSize:                request.PageSize,
		PageToken:               strings.Clone(request.PageCursor),
		RequiredCatalogRevision: cloneRuntimeUint64(request.RequiredCatalogRevision),
		IncludeTotal:            request.IncludeTotal,
		StateFilters:            states,
		TextFilter:              textFilter,
		SortBy:                  sortBy,
		Direction:               direction,
	}, nil
}

func controlAppSort(value server.AppAdministrationSort) (control.AppSortBy, error) {
	switch value {
	case server.AppAdministrationSortDisplayName:
		return control.AppSortByDisplayName, nil
	case server.AppAdministrationSortCreatedAt:
		return control.AppSortByCreatedAt, nil
	case server.AppAdministrationSortUpdatedAt:
		return control.AppSortByUpdatedAt, nil
	default:
		return "", server.ErrAppAdministrationInvalidArgument
	}
}

func controlAppState(value server.AppAdministrationState) (control.AppState, error) {
	switch value {
	case server.AppAdministrationStateActive:
		return control.AppStateActive, nil
	case server.AppAdministrationStateArchived:
		return control.AppStateArchived, nil
	default:
		return "", server.ErrAppAdministrationInvalidArgument
	}
}

func serverAppWorkspace(
	workspace control.AppWorkspace,
) (server.AppAdministrationWorkspace, error) {
	state, err := serverAppState(workspace.State)
	if err != nil {
		return server.AppAdministrationWorkspace{}, err
	}
	description := cloneRuntimeOptionalNonemptyString(workspace.Definition.Description)
	return server.AppAdministrationWorkspace{
		AppID:   strings.Clone(workspace.ID),
		Version: workspace.Version,
		Definition: server.AppAdministrationDefinition{
			Slug:              strings.Clone(workspace.Definition.Slug),
			DisplayName:       strings.Clone(workspace.Definition.DisplayName),
			Description:       description,
			DefaultIndexNames: slices.Clone(workspace.Definition.DefaultIndexes),
			DefaultTimeRange:  serverAppTimeRange(workspace.Definition.DefaultTimeRange),
		},
		State:     state,
		CreatedAt: workspace.CreatedAt,
		UpdatedAt: workspace.UpdatedAt,
	}, nil
}

func serverAppState(value control.AppState) (server.AppAdministrationState, error) {
	switch value {
	case control.AppStateActive:
		return server.AppAdministrationStateActive, nil
	case control.AppStateArchived:
		return server.AppAdministrationStateArchived, nil
	default:
		return "", errors.New("convert app catalog result: persisted app state is invalid")
	}
}

func serverAppTimeRange(
	value *control.AppTimeRange,
) *server.AppAdministrationTimeRange {
	if value == nil {
		return nil
	}
	return &server.AppAdministrationTimeRange{
		Earliest: cloneRuntimeString(value.Earliest),
		Latest:   cloneRuntimeString(value.Latest),
		Timezone: cloneRuntimeString(value.Timezone),
	}
}

func serverAppListResult(
	result control.AppListResult,
) (server.AppAdministrationListResult, error) {
	apps := make([]server.AppAdministrationWorkspace, len(result.Apps))
	for index, app := range result.Apps {
		converted, err := serverAppWorkspace(app)
		if err != nil {
			return server.AppAdministrationListResult{}, err
		}
		apps[index] = converted
	}
	nextPageCursor := ""
	if result.NextPageToken != nil {
		nextPageCursor = strings.Clone(*result.NextPageToken)
	}
	return server.AppAdministrationListResult{
		Apps:            apps,
		CatalogRevision: result.CatalogRevision,
		NextPageCursor:  nextPageCursor,
		TotalSize:       cloneRuntimeUint64(result.TotalSize),
		TotalSizeExact:  result.TotalSizeExact,
	}, nil
}

func mapRuntimeAppCatalogError(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, control.ErrInvalidArgument):
		return server.ErrAppAdministrationInvalidArgument
	case errors.Is(err, control.ErrNotFound):
		return server.ErrAppAdministrationNotFound
	case errors.Is(err, control.ErrAlreadyExists):
		return server.ErrAppAdministrationAlreadyExists
	case errors.Is(err, control.ErrVersionConflict),
		errors.Is(err, control.ErrImmutableSlug),
		errors.Is(err, control.ErrImmutableName),
		errors.Is(err, control.ErrDependencyConflict):
		return server.ErrAppAdministrationConflict
	case errors.Is(err, control.ErrCapacityExceeded):
		return server.ErrAppAdministrationCapacity
	case errors.Is(err, control.ErrPageInvalidated):
		return server.ErrAppAdministrationInvalidPageToken
	default:
		// Unknown persistence and corruption failures retain their identity for
		// server-side diagnostics. The HTTP layer converts them to one stable,
		// non-sensitive unavailable response.
		return err
	}
}

func cloneRuntimeOptionalNonemptyString(value string) *string {
	if value == "" {
		return nil
	}
	result := strings.Clone(value)
	return &result
}

func cloneRuntimeString(value *string) *string {
	if value == nil {
		return nil
	}
	result := strings.Clone(*value)
	return &result
}

func cloneRuntimeUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}
