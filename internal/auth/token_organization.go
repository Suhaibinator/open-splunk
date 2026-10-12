package auth

import (
	"gorm.io/gorm"
)

func (store *Store) resolveTokenOrganizationScopes(database *gorm.DB, organizationID int64, names []string) ([]string, error) {
	ids, err := resolveCollectorTokenScopes(database, names)
	if err != nil || !store.requireOrganizationOwnership {
		return ids, err
	}
	var owned int64
	if err := database.Table("os_index_organizations").Where("organization_id = ? AND index_id IN ?", organizationID, ids).Count(&owned).Error; err != nil {
		return nil, err
	}
	if owned != int64(len(ids)) {
		return nil, errCollectorTokenScopeUnavailable
	}
	return ids, nil
}
