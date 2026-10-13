package common_persistence

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ErrInvalidActiveOrganizationScope indicates that an application supplied a
// table or column expression where ActiveOrganizationScope requires a plain
// SQL identifier.
var ErrInvalidActiveOrganizationScope = errors.New("invalid active organization scope")

const activeOrganizationJoinAlias = "active_org"

// ActiveOrganizationScope returns a GORM scope that joins an application-owned
// table to the active go-common organization row referenced by its organization
// ID column. The inner join deliberately hides rows while their organization is
// soft-deleted and reveals them again if the organization is restored.
//
// sourceTable and organizationIDColumn are SQL identifiers matching
// [A-Za-z_][A-Za-z0-9_]*, not expressions. Keeping them separate lets the scope
// validate them before constructing the join and prevents consumer-provided
// values from becoming executable SQL.
//
// Example:
//
//	db.Model(&Post{}).
//		Scopes(common_persistence.ActiveOrganizationScope("posts", "organization_id"))
func ActiveOrganizationScope(sourceTable, organizationIDColumn string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if !validScopeIdentifier(sourceTable) || !validScopeIdentifier(organizationIDColumn) {
			cause := fmt.Errorf(
				"%w: source table and organization ID column must be plain SQL identifiers",
				ErrInvalidActiveOrganizationScope)
			db.AddError(appLogger.InvariantError("active_organization_scope_uses_plain_identifiers", cause,
				zap.String(logkeys.Stage, "build_active_organization_scope"),
				zap.String(appLogger.KeySourceTable, sourceTable),
				zap.String(appLogger.KeyOrganizationIDColumn, organizationIDColumn),
				zap.String(logkeys.Expected, "plain SQL identifiers"),
				zap.String(logkeys.Actual, "invalid identifier")))
			return db
		}

		return db.Joins(fmt.Sprintf(
			"INNER JOIN %s AS %s ON %s.id = %s.%s AND %s.deleted_at IS NULL",
			common_persistence_models.TableNameOrganizations,
			activeOrganizationJoinAlias,
			activeOrganizationJoinAlias,
			sourceTable,
			organizationIDColumn,
			activeOrganizationJoinAlias,
		))
	}
}

func validScopeIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if index == 0 {
			if character != '_' && !asciiLetter(character) {
				return false
			}
			continue
		}
		if character != '_' && !asciiLetter(character) && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func asciiLetter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}
