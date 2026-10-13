package collectorfleet

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// RestrictIndexes seals a detached index policy for browser administration.
// A collector spanning organizations requires authority over all its indexes.
func RestrictIndexes(tenantID, identity string, names []string) (Scope, error) {
	names = slices.Clone(names)
	if names == nil {
		names = []string{}
	}
	for _, name := range names {
		canonical, err := control.NormalizeIndexName(name)
		if err != nil || canonical != name {
			return Scope{}, invalid("collector access index is invalid")
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
	payload, err := json.Marshal(names)
	if err != nil {
		return Scope{}, err
	}
	return normalizeScope(Scope{TenantID: tenantID, IndexAccess: string(payload), AccessIdentity: identity})
}

func restrictCollectorIndexes(query *gorm.DB, table, access string) *gorm.DB {
	if access == "" {
		return query
	}
	// table is an internal constant, never caller-authored SQL.
	predicate := `EXISTS (SELECT 1 FROM collector_authorized_indexes source
		WHERE source.tenant_id = TABLE.tenant_id AND source.collector_id = TABLE.collector_id)
		AND NOT EXISTS (SELECT 1 FROM collector_authorized_indexes source
		WHERE source.tenant_id = TABLE.tenant_id AND source.collector_id = TABLE.collector_id
		AND NOT (source.index_name IN (SELECT jsonb_array_elements_text(?::jsonb))))`
	return query.Where(strings.ReplaceAll(predicate, "TABLE", table), access)
}

func requireCollectorIndexAccess(database *gorm.DB, scope Scope, collectorID string) error {
	if scope.IndexAccess == "" {
		return nil
	}
	var count int64
	err := restrictCollectorIndexes(database.Model(&fleetRecord{}), "collector_fleet", scope.IndexAccess).
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).Count(&count).Error
	if err != nil {
		return err
	}
	if count != 1 {
		return control.ErrNotFound
	}
	return nil
}
