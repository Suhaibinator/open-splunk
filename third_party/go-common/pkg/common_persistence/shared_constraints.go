package common_persistence

import (
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

type sharedConstraint struct{ table, name, definition string }

func sharedOwnershipConstraints() []sharedConstraint {
	var constraints []sharedConstraint
	for _, tables := range [][]organizationOwnedTable{chatAgentOrganizationOwnedTables, inferenceAttemptOrganizationOwnedTables} {
		for _, owned := range tables {
			constraints = append(constraints,
				sharedConstraint{owned.table, owned.checkConstraint, "CHECK (organization_id IS NOT NULL AND organization_id > 0)"},
				sharedConstraint{owned.table, owned.foreignKey, "FOREIGN KEY (organization_id) REFERENCES gocommon_organizations(id) ON DELETE RESTRICT"})
		}
	}
	return append(constraints,
		sharedConstraint{"gocommon_inference_operation_routes", inferenceOperationRouteModelConstraint, "FOREIGN KEY (inference_model_id) REFERENCES gocommon_inference_models(id) ON DELETE RESTRICT"},
		sharedConstraint{"gocommon_inference_model_rate_limit_policies", inferenceModelPolicyModelConstraint, "FOREIGN KEY (inference_model_id) REFERENCES gocommon_inference_models(id) ON DELETE CASCADE"})
}

// Scope by table OID: constraint names may repeat in application-owned schemas.
func existingSharedConstraint(db *gorm.DB, want sharedConstraint) (bool, error) {
	var found []struct {
		Definition string
		Valid      bool
	}
	if err := db.Raw(`SELECT pg_get_constraintdef(c.oid) AS definition,c.convalidated AS valid FROM pg_constraint c
WHERE c.conrelid=to_regclass(format('%I.%I',current_schema(),?::text)) AND c.conname=?`, want.table, want.name).Scan(&found).Error; err != nil {
		return false, err
	}
	if len(found) == 0 {
		return false, nil
	}
	var schema string
	if err := db.Raw("SELECT current_schema()").Scan(&schema).Error; err != nil {
		return false, err
	}
	actual := strings.ReplaceAll(found[0].Definition, schema+".", "")
	actual = strings.ReplaceAll(actual, fmt.Sprintf(`"%s".`, schema), "")
	if !found[0].Valid || normalizeSchemaDefinition(actual) != normalizeSchemaDefinition(want.definition) {
		return true, fmt.Errorf("%w: conflicting or unvalidated constraint %s on %s (found %s; expected %s)", ErrManualMigrationRequired, want.name, want.table, actual, want.definition)
	}
	return true, nil
}

func validateSharedConstraint(db *gorm.DB, want sharedConstraint) error {
	_, err := existingSharedConstraint(db, want)
	return err
}

func executeSharedConstraint(db *gorm.DB, want sharedConstraint) error {
	exists, err := existingSharedConstraint(db, want)
	if err != nil || exists {
		return err
	}
	return db.Exec(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", want.table, want.name, want.definition)).Error
}

var sharedConstraintDDL = regexp.MustCompile(`^ALTER TABLE ([a-z0-9_]+) ADD CONSTRAINT ([a-z0-9_]+) (.+)$`)

func parseSharedConstraint(statement string) (sharedConstraint, bool) {
	match := sharedConstraintDDL.FindStringSubmatch(statement)
	if match == nil {
		return sharedConstraint{}, false
	}
	return sharedConstraint{match[1], match[2], match[3]}, true
}
