package knowledgecatalog

import (
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"gorm.io/gorm"
)

// Scalar and ledger admission stays deployment-wide. Semantic programs use
// only the immutable organization namespace of each defining app.
func publicationIndexOrganizationInventories(tx *gorm.DB, facts publicationIndexAdmissionTenantFacts, names []string, candidate string) ([]publicationIndexNameAdmissionInventory, error) {
	appOrganizations := make(map[string]int64, len(facts.apps.records))
	groups := make(map[int64]*publicationIndexAdmissionTenantFacts)
	for _, app := range facts.apps.records {
		appOrganizations[app.AppID] = app.OrganizationID
		group := groups[app.OrganizationID]
		if group == nil {
			group = &publicationIndexAdmissionTenantFacts{driver: facts.driver}
			groups[app.OrganizationID] = group
		}
		if slices.Contains(facts.apps.active, app.AppID) {
			group.apps.active = append(group.apps.active, app.AppID)
		}
	}
	scalars := make(map[string]publicationTransitionProjectionScalar, len(facts.projectionScalars))
	for _, scalar := range facts.projectionScalars {
		scalars[scalar.knowledgeObjectID] = scalar
	}
	for _, winner := range facts.winners {
		organizationID := appOrganizations[winner.object.AppID]
		group := groups[organizationID]
		scalar, found := scalars[winner.object.KnowledgeObjectID]
		if organizationID <= 0 || group == nil || !found {
			return nil, fmt.Errorf("%w: semantic organization inventory authority is missing", ErrCorrupt)
		}
		group.winners = append(group.winners, winner)
		group.aggregate.definitionBytes += scalar.definitionBytes
		group.aggregate.projectionBytes += scalar.projectionBytes
		group.aggregate.selectorPatterns += scalar.selectorPatterns
		group.aggregate.selectorValueBytes += scalar.selectorValueBytes
		group.aggregate.canonicalSelectorBytes += scalar.canonicalSelectorBytes
		group.aggregate.dependencies += scalar.dependencyCount
		definition, err := knowledgedefinition.Normalize(winner.object.Definition)
		if err != nil {
			return nil, fmt.Errorf("%w: semantic organization definition is invalid", ErrCorrupt)
		}
		group.selectorWork += definition.Selector.Stats().WildcardWorkUnits
	}
	organizations := make([]int64, 0, len(groups))
	for organizationID, group := range groups {
		if len(group.winners) > 0 {
			organizations = append(organizations, organizationID)
		}
	}
	slices.Sort(organizations)
	result := make([]publicationIndexNameAdmissionInventory, 0, len(organizations))
	for _, organizationID := range organizations {
		owned, err := publicationOrganizationIndexNames(tx, organizationID, names)
		if err != nil {
			return nil, err
		}
		result = append(result, publicationIndexNameInventory(*groups[organizationID], publicationIndexAdmissionIndexFacts{names: owned}, candidate))
	}
	return result, nil
}
