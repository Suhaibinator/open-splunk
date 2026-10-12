package dashboards

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

const (
	maximumDashboardsPerOwner = 64
	maximumVisibleDashboards  = 10000
	maximumIDAttempts         = 4
)

// AccessScope is the authenticated owner namespace. Object identifiers never
// select a different owner.
type AccessScope struct {
	OwnerID           string
	OrganizationID    string
	SiteAdministrator bool
}

type Options struct {
	Clock       func() time.Time
	IDGenerator func() (string, error)
	TenantID    string
}

// Store owns bounded dashboard persistence over the configured control DB.
type Store struct {
	orm         *gorm.DB
	clock       func() time.Time
	idGenerator func() (string, error)
	tenantID    string
}

func New(db *control.DB, options Options) (*Store, error) {
	if db == nil || db.GORMDB() == nil {
		return nil, fmt.Errorf("%w: control database is required", control.ErrInvalidArgument)
	}
	tenantID, err := canonicalRequiredText("tenant ID", options.TenantID, maximumDashboardOwnerIDBytes)
	if err != nil {
		return nil, err
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = newID
	}
	return &Store{orm: db.GORMDB(), clock: clock, idGenerator: idGenerator, tenantID: tenantID}, nil
}

func (store *Store) Create(ctx context.Context, scope AccessScope, input *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
	ownerID, err := owner(ctx, scope)
	if err != nil {
		return nil, err
	}
	if scope.OrganizationID == "" {
		return nil, fmt.Errorf("%w: organization is required for dashboard creation", control.ErrInvalidArgument)
	}
	definition, indexed, encoded, err := normalizeDefinition(input, ownerID)
	if err != nil {
		return nil, err
	}
	now, err := normalizeTime(store.clock())
	if err != nil {
		return nil, err
	}
	var count int64
	if err := store.orm.WithContext(ctx).Model(&dashboardRecord{}).Where("organization_id = ? AND tenant_id = ? AND owner_id = ?", scopeOrganization(scope), store.tenantID, ownerID).Count(&count).Error; err != nil {
		return nil, mapContext(ctx, "count dashboards", err)
	}
	if count >= maximumDashboardsPerOwner {
		return nil, control.ErrCapacityExceeded
	}
	for range maximumIDAttempts {
		id, generationErr := store.idGenerator()
		if generationErr != nil || validateDashboardID(id) != nil {
			return nil, errors.New("generate dashboard ID: invalid generator result")
		}
		record := dashboardRecord{
			DashboardID: id, Version: 1, Name: indexed.name, AppID: indexed.appID,
			SourceIndexNames: indexed.sourceIndexNames,
			TenantID:         store.tenantID, OrganizationID: scopeOrganization(scope), OwnerID: ownerID, SharingScope: int64(indexed.sharingScope), DefinitionProto: encoded,
			CreatedAtUnixMicro: now.UnixMicro(), UpdatedAtUnixMicro: now.UnixMicro(),
		}
		if createErr := store.orm.WithContext(ctx).Create(&record).Error; createErr == nil {
			return build(id, 1, definition, now, now), nil
		} else if ctx.Err() != nil {
			return nil, ctx.Err()
		} else if strings.Contains(createErr.Error(), "dashboard owner capacity exhausted") {
			return nil, control.ErrCapacityExceeded
		}
		conflict, conflictErr := store.nameConflict(ctx, scopeOrganization(scope), ownerID, indexed.appID, indexed.name, "")
		if conflictErr != nil {
			return nil, conflictErr
		}
		if conflict {
			return nil, fmt.Errorf("%w: dashboard %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
		}
		var existing int64
		if err := store.orm.WithContext(ctx).Model(&dashboardRecord{}).Where("dashboard_id = ?", id).Count(&existing).Error; err != nil {
			return nil, mapContext(ctx, "check dashboard ID", err)
		}
		if existing == 0 {
			return nil, errors.New("create dashboard: database rejected the record")
		}
	}
	return nil, errors.New("create dashboard: repeated random ID collision")
}

func (store *Store) Get(ctx context.Context, scope AccessScope, id string) (*opensplunk.Dashboard, error) {
	ownerID, err := owner(ctx, scope)
	if err != nil {
		return nil, err
	}
	if err := validateDashboardID(id); err != nil {
		return nil, err
	}
	var record dashboardRecord
	err = dashboardScopeQuery(store.orm.WithContext(ctx), scope, ownerID, true).Where("dashboard_id = ? AND tenant_id = ?", id, store.tenantID).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, control.ErrNotFound
	}
	if err != nil {
		return nil, mapContext(ctx, "get dashboard", err)
	}
	return fromRecord(record, store.tenantID)
}

func (store *Store) List(ctx context.Context, scope AccessScope, appID *string) ([]*opensplunk.Dashboard, error) {
	ownerID, err := owner(ctx, scope)
	if err != nil {
		return nil, err
	}
	query := dashboardScopeQuery(store.orm.WithContext(ctx), scope, ownerID, true).Where("tenant_id = ?", store.tenantID)
	if appID != nil {
		value, normalizationErr := canonicalRequiredText("dashboard app ID filter", *appID, maximumDashboardAppIDBytes)
		if normalizationErr != nil {
			return nil, normalizationErr
		}
		query = query.Where("app_id = ?", value)
	}
	var records []dashboardRecord
	if err := query.Order("updated_at_unix_micro DESC, dashboard_id DESC").Limit(maximumVisibleDashboards + 1).Find(&records).Error; err != nil {
		return nil, mapContext(ctx, "list dashboards", err)
	}
	if len(records) > maximumVisibleDashboards {
		return nil, control.ErrCapacityExceeded
	}
	ownerCounts := make(map[string]int)
	for _, record := range records {
		key := strconv.FormatInt(record.OrganizationID, 10) + ":" + record.OwnerID
		ownerCounts[key]++
		if ownerCounts[key] > maximumDashboardsPerOwner {
			return nil, errors.New("list dashboards: persisted owner capacity invariant exceeded")
		}
	}
	result := make([]*opensplunk.Dashboard, len(records))
	for index := range records {
		result[index], err = fromRecord(records[index], store.tenantID)
		if err != nil {
			return nil, fmt.Errorf("list dashboards: %w", err)
		}
	}
	return result, nil
}

func (store *Store) Update(ctx context.Context, scope AccessScope, id string, expectedVersion uint64, input *opensplunk.DashboardDefinition) (*opensplunk.Dashboard, error) {
	ownerID, err := owner(ctx, scope)
	if err != nil {
		return nil, err
	}
	if err := validateDashboardID(id); err != nil {
		return nil, err
	}
	storedExpectedVersion, err := validateVersion(expectedVersion)
	if err != nil {
		return nil, err
	}
	current, err := store.Get(ctx, scope, id)
	if err != nil {
		return nil, err
	}
	if !scope.SiteAdministrator && current.GetDefinition().GetOwnerId() != ownerID {
		return nil, control.ErrNotFound
	}
	ownerID = current.GetDefinition().GetOwnerId()
	definition, indexed, encoded, err := normalizeDefinition(input, ownerID)
	if err != nil {
		return nil, err
	}

	if current.GetVersion() != expectedVersion {
		return nil, control.ErrVersionConflict
	}
	conflict, err := store.nameConflict(ctx, scopeOrganization(scope), ownerID, indexed.appID, indexed.name, id)
	if err != nil {
		return nil, err
	}
	if conflict {
		return nil, fmt.Errorf("%w: dashboard %q already exists in app %q", control.ErrAlreadyExists, indexed.name, indexed.appID)
	}
	now, err := nextTime(store.clock(), current.GetUpdatedAt().AsTime())
	if err != nil {
		return nil, err
	}
	updated := dashboardScopeQuery(store.orm.WithContext(ctx), scope, ownerID, false).Model(&dashboardRecord{}).
		Where("dashboard_id = ? AND tenant_id = ? AND owner_id = ? AND version = ?", id, store.tenantID, ownerID, storedExpectedVersion).
		Updates(map[string]any{
			"version": gorm.Expr("version + 1"), "name": indexed.name, "app_id": indexed.appID,
			"sharing_scope": int64(indexed.sharingScope), "definition_proto": encoded,
			"source_index_names":    indexed.sourceIndexNames,
			"updated_at_unix_micro": now.UnixMicro(),
		})
	if updated.Error != nil {
		return nil, mapContext(ctx, "update dashboard", updated.Error)
	}
	if updated.RowsAffected != 1 {
		return nil, control.ErrVersionConflict
	}
	return build(id, expectedVersion+1, definition, current.GetCreatedAt().AsTime(), now), nil
}

func (store *Store) Delete(ctx context.Context, scope AccessScope, id string, expectedVersion uint64) error {
	ownerID, err := owner(ctx, scope)
	if err != nil {
		return err
	}
	if err := validateDashboardID(id); err != nil {
		return err
	}
	storedExpectedVersion, err := validateVersion(expectedVersion)
	if err != nil {
		return err
	}
	var current dashboardRecord
	err = dashboardScopeQuery(store.orm.WithContext(ctx), scope, ownerID, false).Select("dashboard_id", "version", "owner_id").Where("dashboard_id = ? AND tenant_id = ?", id, store.tenantID).Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return control.ErrNotFound
	}
	if err != nil {
		return mapContext(ctx, "read dashboard for delete", err)
	}
	ownerID = current.OwnerID
	if current.Version != storedExpectedVersion {
		return control.ErrVersionConflict
	}
	deleted := dashboardScopeQuery(store.orm.WithContext(ctx), scope, ownerID, false).Where("dashboard_id = ? AND tenant_id = ? AND owner_id = ? AND version = ?", id, store.tenantID, ownerID, storedExpectedVersion).Delete(&dashboardRecord{})
	if deleted.Error != nil {
		return mapContext(ctx, "delete dashboard", deleted.Error)
	}
	if deleted.RowsAffected != 1 {
		return control.ErrVersionConflict
	}
	return nil
}

func (store *Store) nameConflict(ctx context.Context, organizationID int64, ownerID, appID, name, exceptID string) (bool, error) {
	query := store.orm.WithContext(ctx).Model(&dashboardRecord{}).Where("tenant_id = ? AND owner_id = ? AND app_id = ? AND name = ?", store.tenantID, ownerID, appID, name)
	if exceptID != "" {
		query = query.Where("organization_id = (SELECT organization_id FROM dashboards WHERE dashboard_id = ?)", exceptID)
	} else {
		query = query.Where("organization_id = ?", organizationID)
	}
	if exceptID != "" {
		query = query.Where("dashboard_id <> ?", exceptID)
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return false, mapContext(ctx, "check dashboard name", err)
	}
	return count != 0, nil
}

func fromRecord(record dashboardRecord, tenantID string) (*opensplunk.Dashboard, error) {
	if validateDashboardID(record.DashboardID) != nil || record.TenantID != tenantID || record.Version < 1 || record.UpdatedAtUnixMicro < record.CreatedAtUnixMicro {
		return nil, errors.New("invalid dashboard record")
	}
	definition := new(opensplunk.DashboardDefinition)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.DefinitionProto, definition); err != nil {
		return nil, errors.New("invalid persisted dashboard definition")
	}
	normalized, indexed, encoded, err := normalizeDefinition(definition, record.OwnerID)
	if err != nil || !resourcepolicy.MatchesSourceNames(record.SourceIndexNames, indexed.sourceIndexNames) || !proto.Equal(normalized, definition) || indexed.name != record.Name ||
		indexed.appID != record.AppID || int64(indexed.sharingScope) != record.SharingScope ||
		!bytes.Equal(encoded, record.DefinitionProto) {
		return nil, errors.New("dashboard record does not match its definition")
	}
	created := time.UnixMicro(record.CreatedAtUnixMicro).UTC()
	updated := time.UnixMicro(record.UpdatedAtUnixMicro).UTC()
	if _, err := normalizeTime(created); err != nil {
		return nil, err
	}
	if _, err := normalizeTime(updated); err != nil {
		return nil, err
	}
	return build(record.DashboardID, uint64(record.Version), definition, created, updated), nil
}

func build(id string, version uint64, definition *opensplunk.DashboardDefinition, created, updated time.Time) *opensplunk.Dashboard {
	return &opensplunk.Dashboard{
		DashboardId: id, Version: version, Definition: proto.Clone(definition).(*opensplunk.DashboardDefinition),
		CreatedAt: timestamppb.New(created), UpdatedAt: timestamppb.New(updated),
	}
}

func owner(ctx context.Context, scope AccessScope) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("%w: context is required", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !scope.SiteAdministrator || scope.OrganizationID != "" {
		id, err := strconv.ParseInt(scope.OrganizationID, 10, 64)
		if err != nil || id < 1 || strconv.FormatInt(id, 10) != scope.OrganizationID {
			return "", fmt.Errorf("%w: organization ID must be canonical positive decimal", control.ErrInvalidArgument)
		}
	}
	return canonicalRequiredText("owner ID", scope.OwnerID, maximumDashboardOwnerIDBytes)
}

func mapContext(ctx context.Context, operation string, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%s: %w", operation, ctx.Err())
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func newID() (string, error) {
	bytes := make([]byte, 18)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "dash_" + base64.RawURLEncoding.EncodeToString(bytes), nil
}

func scopeOrganization(scope AccessScope) int64 {
	id, _ := strconv.ParseInt(scope.OrganizationID, 10, 64)
	return id
}
func dashboardScopeQuery(query *gorm.DB, scope AccessScope, ownerID string, read bool) *gorm.DB {
	if read {
		query = resourcepolicy.Filter(query, "dashboards")
	}
	if !scope.SiteAdministrator || scope.OrganizationID != "" {
		query = query.Where("organization_id = ?", scopeOrganization(scope))
	}
	if scope.SiteAdministrator {
		return query
	}
	if read {
		return query.Where("(owner_id = ? OR sharing_scope IN (2, 3))", ownerID)
	}
	return query.Where("owner_id = ?", ownerID)
}
