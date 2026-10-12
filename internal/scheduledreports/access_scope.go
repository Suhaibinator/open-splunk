package scheduledreports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"gorm.io/gorm"
)

type AccessScope = savedobjects.AccessScope

func validateScope(scope AccessScope) error {
	if err := savedobjects.ValidateAccessScope(scope); err != nil {
		return ErrInvalidArgument
	}
	return nil
}
func scopedScheduleQuery(database *gorm.DB, scope AccessScope) *gorm.DB {
	if err := validateScope(scope); err != nil {
		return database.Where("FALSE")
	}
	if scope.OrganizationID != "" {
		database = database.Where("organization_id = ?", scope.OrganizationID)
	}
	if !scope.SiteAdministrator {
		database = database.Where("owner_id = ?", scope.OwnerID)
	}
	return database
}
func (repository *Repository) sourceScope(ctx context.Context, scope AccessScope) (AccessScope, error) {
	if repository.resolveSourceScope != nil {
		return repository.resolveSourceScope(ctx, scope)
	}
	return scope, validateScope(scope)
}

func scopeCursorIdentity(scope AccessScope) string {
	digest := sha256.Sum256([]byte(scope.OwnerID + "\x00" + scope.OrganizationID + "\x00" + strconv.FormatBool(scope.SiteAdministrator)))
	return hex.EncodeToString(digest[:])
}

func policyCursorIdentity(ctx context.Context, scope AccessScope) (string, error) {
	fingerprint, err := resourcepolicy.Fingerprint(ctx)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(scopeCursorIdentity(scope) + "\x00" + fingerprint))
	return hex.EncodeToString(digest[:]), nil
}
func (repository *Repository) authorizeMetadataSource(ctx context.Context, scope AccessScope, id string) error {
	_, present, err := resourcepolicy.FromContext(ctx)
	if err != nil {
		return err
	}
	if !present || scope.SiteAdministrator {
		return nil
	}
	_, err = savedobjects.GetInTransaction(ctx, repository.orm.WithContext(ctx), scope, id)
	if errors.Is(err, control.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
func (repository *Repository) visibleMetadataSources(ctx context.Context, query *gorm.DB, scope AccessScope) *gorm.DB {
	_, present, err := resourcepolicy.FromContext(ctx)
	if err != nil {
		// AddError retains the joined error on the returned GORM query.
		_ = query.AddError(err)
		return query.Where("FALSE")
	}
	if !present || scope.SiteAdministrator {
		return query
	}
	sources := repository.orm.WithContext(ctx).Table("saved_searches").Select("saved_search_id").Where("organization_id = ? AND (owner_id = ? OR sharing_scope IN ?)", scope.OrganizationID, scope.OwnerID, []int64{2, 3})
	sources = resourcepolicy.Filter(sources, "saved_searches")
	return query.Where("saved_search_id IN (?)", sources)
}
