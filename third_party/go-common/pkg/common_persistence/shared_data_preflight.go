package common_persistence

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

var sharedUniqueIndex = regexp.MustCompile(`(?i)^CREATE UNIQUE INDEX IF NOT EXISTS "?([a-z0-9_]+)"? ON "?([a-z0-9_]+)"? \(([^)]+)\)(?: WHERE (.+))?$`)

// Missing unique indexes are additive only when current data satisfies them.
// Read-only checks precede all DDL, including compatible column additions.
func requireSharedData(db *gorm.DB) error {
	for _, statement := range sharedRawSchemaStatements() {
		match := sharedUniqueIndex.FindStringSubmatch(statement)
		if match == nil {
			continue
		}
		var exists bool
		if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", match[2]).Scan(&exists).Error; err != nil {
			return err
		}
		if !exists {
			continue
		}
		// Preflight already validated present indexes. Their uniqueness is
		// enforced by PostgreSQL, so avoid scanning their full data on every boot.
		var indexed bool
		if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", match[1]).Scan(&indexed).Error; err != nil {
			return err
		}
		if indexed {
			continue
		}
		predicate := match[4]
		if predicate == "" {
			predicate = "TRUE"
		}
		predicate = "(" + predicate + ")"
		// Ordinary unique indexes permit repeated NULL keys.
		for _, key := range strings.Split(match[3], ",") {
			predicate += " AND " + strings.TrimSpace(key) + " IS NOT NULL"
		}
		var duplicate bool
		err := db.Raw(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s GROUP BY %s HAVING count(*)>1)", match[2], predicate, match[3])).Scan(&duplicate).Error
		if err != nil {
			var pgErr *pgconn.PgError
			// An absent compatible column will be created by AutoMigrate. The
			// breaking column shapes are checked separately before this step.
			if errors.As(err, &pgErr) && pgErr.Code == "42703" {
				continue
			}
			return err
		}
		if duplicate {
			return fmt.Errorf("%w: duplicate keys for %s; resolve records before startup", ErrManualMigrationRequired, match[1])
		}
	}
	for _, tables := range [][]organizationOwnedTable{chatAgentOrganizationOwnedTables, inferenceAttemptOrganizationOwnedTables} {
		for _, owned := range tables {
			var exists bool
			if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", owned.table).Scan(&exists).Error; err != nil {
				return err
			}
			if !exists {
				continue
			}
			var invalid bool
			if err := db.Raw(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s r WHERE r.organization_id IS NULL OR r.organization_id<=0 OR NOT EXISTS(SELECT 1 FROM gocommon_organizations o WHERE o.id=r.organization_id))", owned.table)).Scan(&invalid).Error; err != nil {
				return err
			}
			if invalid {
				return fmt.Errorf("%w: ambiguous or invalid organization ownership in %s", ErrManualMigrationRequired, owned.table)
			}
		}
	}
	for _, table := range []string{"gocommon_inference_operation_routes", "gocommon_inference_model_rate_limit_policies"} {
		var exists bool
		if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", table).Scan(&exists).Error; err != nil {
			return err
		}
		if !exists {
			continue
		}
		var invalid bool
		if err := db.Raw(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s r WHERE NOT EXISTS(SELECT 1 FROM gocommon_inference_models m WHERE m.id=r.inference_model_id))", table)).Scan(&invalid).Error; err != nil {
			return err
		}
		if invalid {
			return fmt.Errorf("%w: orphaned model references in %s", ErrManualMigrationRequired, table)
		}
	}
	return nil
}
