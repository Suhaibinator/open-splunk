package server

import (
	"context"
	"errors"
	"fmt"
	"slices"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
)

func (handler *apiHandler) metadataReadSnapshot(ctx context.Context) (resourcepolicy.Snapshot, error) {
	scope, ok := authorization.FromContext(ctx)
	if !ok {
		return resourcepolicy.Snapshot{}, authorization.ErrUnauthorized
	}
	snapshot := resourcepolicy.Snapshot{IndexNames: []string{}, AvailableIndexNames: []string{}, AppIDs: []string{}, AppDefaultIndexNames: map[string][]string{}}
	if handler.authorization.SupportsVisibilitySnapshot() {
		visibility, err := handler.authorization.IndexVisibility(ctx, scope.Subject, scope.OrganizationID)
		if err != nil {
			return snapshot, err
		}
		if visibility.Subject.UserType != scope.Subject.UserType {
			return snapshot, authorization.ErrConflict
		}
		snapshot.Fingerprint = visibility.Fingerprint
		snapshot.SiteAdministrator = visibility.Subject.IsSiteAdmin()
		for _, index := range visibility.Indexes {
			snapshot.AvailableIndexNames = append(snapshot.AvailableIndexNames, index.Name)
			if index.SearchAllowed {
				snapshot.IndexNames = append(snapshot.IndexNames, index.Name)
			}
		}
	} else {
		snapshot.SiteAdministrator = scope.Subject.IsSiteAdmin()
		snapshot.Fingerprint = fmt.Sprintf("%d:%d:%d", scope.Subject.UserID, scope.Subject.UserType, scope.OrganizationID)
		if !snapshot.SiteAdministrator {
			records, err := handler.indexes.ListIndexes(ctx)
			if err != nil {
				return snapshot, err
			}
			if len(records) > 1024 {
				return snapshot, authorization.ErrInvalidPolicy
			}
			for _, record := range records {
				org, err := handler.authorization.OrganizationForIndex(ctx, record.ID)
				if err != nil && !errors.Is(err, authorization.ErrUnassigned) {
					return snapshot, err
				}
				if org != scope.OrganizationID {
					continue
				}
				snapshot.AvailableIndexNames = append(snapshot.AvailableIndexNames, record.Definition.Name)
				if err = handler.authorizeIndexCapability(ctx, record.ID, authorization.Search); err == nil && isSearchableIndexRecord(record, record.Definition.Name) {
					snapshot.IndexNames = append(snapshot.IndexNames, record.Definition.Name)
				} else if err != nil && !errors.Is(err, authorization.ErrForbidden) && !errors.Is(err, authorization.ErrUnassigned) {
					return snapshot, err
				}
			}
		}
	}
	if snapshot.SiteAdministrator {
		return snapshot, nil
	}
	if scope.OrganizationID <= 0 {
		return snapshot, authorization.ErrForbidden
	}
	if handler.appCatalog != nil {
		catalog, err := handler.appCatalog.ListActiveApps(ctx, handler.tenantID, maximumBootstrapApps)
		if err != nil {
			return snapshot, err
		}
		if !catalog.Complete {
			return snapshot, authorization.ErrInvalidPolicy
		}
		for _, app := range catalog.Apps {
			snapshot.AppIDs = append(snapshot.AppIDs, app.AppID)
			snapshot.AppDefaultIndexNames[app.AppID] = slices.Clone(app.DefaultIndexNames)
		}
	} else {
		for _, app := range handler.bootstrap.Apps {
			snapshot.AppIDs = append(snapshot.AppIDs, app.GetAppId())
			snapshot.AppDefaultIndexNames[app.GetAppId()] = slices.Clone(app.GetDefaultIndexNames())
		}
	}
	return snapshot, nil
}

func (handler *apiHandler) filterAppDefaults(ctx context.Context, definition *opensplunk.AppDefinition) error {
	snapshot, present, err := resourcepolicy.FromContext(ctx)
	if err != nil {
		return mapAuthorizationError(err)
	}
	if !present || snapshot.SiteAdministrator {
		return nil
	}
	visible := make([]string, 0, len(definition.GetDefaultIndexNames()))
	for _, name := range definition.GetDefaultIndexNames() {
		if slices.Contains(snapshot.IndexNames, name) {
			visible = append(visible, name)
		}
	}
	definition.DefaultIndexNames = visible
	return nil
}
