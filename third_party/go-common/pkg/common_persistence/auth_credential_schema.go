package common_persistence

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
)

// liveRowsPredicate is how PostgreSQL renders the partial-index predicate
// declared by the credential models' `where:deleted_at IS NULL` index tags.
const liveRowsPredicate = "(deleted_at IS NULL)"

// linkedSubjectPredicate is how PostgreSQL renders OAuthLogin's
// `where:subject IS NOT NULL` index tag.
const linkedSubjectPredicate = "(subject IS NOT NULL)"

// credentialUniqueIndex is a unique index a credential table must carry, with
// exactly predicate ("" for a full index), before AutoMigrate runs.
type credentialUniqueIndex struct {
	table     string
	name      string
	columns   []string
	predicate string
}

// credentialUniqueIndexes mirrors the uniqueIndex tags on UserPassword,
// UserAuthMethod, and OAuthLogin; a unit test keeps them in sync.
var credentialUniqueIndexes = []credentialUniqueIndex{
	{table: common_persistence_models.TableNameUserPasswords, name: "idx_gocommon_user_passwords_user_id", columns: []string{"user_id"}, predicate: liveRowsPredicate},
	{table: common_persistence_models.TableNameUserAuthMethods, name: "idx_user_method", columns: []string{"user_id", "method_type"}, predicate: liveRowsPredicate},
	{table: common_persistence_models.TableNameOAuthLogins, name: "idx_oauth_logins_user_provider_slug", columns: []string{"user_id", "provider_slug"}},
	{table: common_persistence_models.TableNameOAuthLogins, name: "idx_oauth_logins_provider_subject", columns: []string{"provider_slug", "subject"}, predicate: linkedSubjectPredicate},
}

// requireCredentialSchema runs before AutoMigrate, while bootstrap
// owns the database advisory lock. AutoMigrate cannot reach the current schema
// on its own: it casts a legacy provider-name method_type column in place, which
// fails on the first stored name with a bare "invalid input syntax for type
// integer" error or silently converts an empty table; it cannot turn the legacy
// integer OAuth provider column into a provider slug; and it matches indexes by
// name only, so it keeps a stale definition where the model declares another
// one. The explicit SQL migration is the only supported conversion, so any such
// table stops startup with an error naming it. Absent tables pass; AutoMigrate
// creates them in the current shape.
func requireCredentialSchema(db *gorm.DB) error {
	var dataType string
	if err := db.Raw(`SELECT data_type FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = ? AND column_name = 'method_type'`,
		common_persistence_models.TableNameUserAuthMethods).Scan(&dataType).Error; err != nil {
		return err
	}
	switch dataType {
	case "", "integer":
	default:
		return fmt.Errorf("%w (found %s.method_type column type %s)",
			ErrManualMigrationRequired, common_persistence_models.TableNameUserAuthMethods, dataType)
	}
	if err := requireOAuthIdentitySchema(db); err != nil {
		return err
	}
	for _, want := range credentialUniqueIndexes {
		if err := requireCredentialUniqueIndex(db, want); err != nil {
			return err
		}
	}
	return nil
}

// requireOAuthIdentitySchema rejects the legacy integer-provider OAuth login
// table. The SQL migration maps it to provider slugs and subjects.
func requireOAuthIdentitySchema(db *gorm.DB) error {
	var columns []string
	if err := db.Raw(`SELECT column_name FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = ? AND column_name IN ('provider', 'provider_slug')`,
		common_persistence_models.TableNameOAuthLogins).Scan(&columns).Error; err != nil {
		return err
	}
	if slices.Contains(columns, "provider") {
		return fmt.Errorf("%w (%s still has the legacy integer provider column)",
			ErrManualMigrationRequired, common_persistence_models.TableNameOAuthLogins)
	}
	return nil
}

func requireCredentialUniqueIndex(db *gorm.DB, want credentialUniqueIndex) error {
	var tableExists bool
	if err := db.Raw(`SELECT EXISTS (SELECT 1 FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = ?)`, want.table).Scan(&tableExists).Error; err != nil {
		return err
	}
	if !tableExists {
		return nil
	}
	var found []struct {
		IsUnique  bool
		IsValid   bool
		Columns   string
		Predicate string
	}
	// Expression columns have attnum 0 and drop out of the join, so an
	// expression index never matches the expected column list.
	if err := db.Raw(`SELECT i.indisunique AS is_unique, (i.indisvalid AND i.indisready AND i.indislive AND i.indexprs IS NULL AND i.indnkeyatts = i.indnatts) AS is_valid,
    coalesce((SELECT string_agg(a.attname, ',' ORDER BY k.ord)
        FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum), '') AS columns,
    coalesce(pg_get_expr(i.indpred, i.indrelid), '') AS predicate
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_class tc ON tc.oid = i.indrelid
JOIN pg_namespace n ON n.oid = tc.relnamespace
WHERE n.nspname = current_schema() AND tc.relname = ? AND ic.relname = ?`, want.table, want.name).Scan(&found).Error; err != nil {
		return err
	}
	if len(found) == 0 {
		return fmt.Errorf("%w (%s has no index %s)", ErrManualMigrationRequired, want.table, want.name)
	}
	index := found[0]
	if !index.IsUnique || !index.IsValid || index.Predicate != want.predicate ||
		!slices.Equal(strings.Split(index.Columns, ","), want.columns) {
		return fmt.Errorf("%w (%s.%s must be a valid unique index on (%s) with predicate %q; found unique=%t valid=%t columns=(%s) predicate=%q)",
			ErrManualMigrationRequired, want.table, want.name, strings.Join(want.columns, ", "), want.predicate,
			index.IsUnique, index.IsValid, index.Columns, index.Predicate)
	}
	return validateSharedDDL(db, want.statement())
}

// statement is the DDL the model's index tags produce, used to validate the
// complete existing definition (sort order, operator class, method).
func (want credentialUniqueIndex) statement() string {
	statement := fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)", want.name, want.table, strings.Join(want.columns, ", "))
	if want.predicate != "" {
		statement += " WHERE " + strings.TrimSuffix(strings.TrimPrefix(want.predicate, "("), ")")
	}
	return statement
}
