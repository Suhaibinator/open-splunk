package common_persistence

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

// ErrManualMigrationRequired identifies a breaking schema change that startup
// must not apply. Startup creates fresh schema and reconciles compatible additions.
var ErrManualMigrationRequired = errors.New("manual migration required; the existing schema conflicts with this release and must be changed with explicit SQL")

// requireCurrentSchema performs no DDL or data writes. Presence checks also protect
// empty existing tables: an empty source schema still requires the explicit upgrade.
func requireCurrentSchema(db *gorm.DB) error {
	if err := requireCredentialSchema(db); err != nil {
		return err
	}
	checks := []struct{ table, column, dataType, defaultValue string }{
		{"gocommon_user_auth_methods", "method_type", "integer", ""},
		{"gocommon_oauth_logins", "provider_slug", "character varying", ""},
		{"gocommon_attachments", "upload_url_expires_at", "timestamp with time zone", "now()"},
		{"gocommon_attachments", "visibility", "integer", "1"},
		{"gocommon_chat_channel_participants", "notification_mode", "integer", "1"},
		{"gocommon_chat_channels", "features", "bigint", "0"},
		{"gocommon_chat_agent_sessions", "status", "smallint", "1"},
		{"gocommon_chat_messages", "kind", "smallint", "1"},
		{"gocommon_chat_messages", "role", "smallint", "1"},
		{"gocommon_chat_messages", "agent_note_subtype", "text", "''::text"},
		{"gocommon_inference_models", "provider_id", "character varying", ""},
		{"gocommon_chat_agent_sessions", "effective_system_prompt", "text", ""},
	}
	for _, check := range checks {
		var columns []struct {
			ColumnDefault        *string
			DataType, IsNullable string
		}
		if err := db.Raw(`SELECT column_default, data_type, is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?`, check.table, check.column).Scan(&columns).Error; err != nil {
			return err
		}
		if len(columns) == 0 {
			var exists bool
			if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", check.table).Scan(&exists).Error; err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("%w: %s.%s is missing", ErrManualMigrationRequired, check.table, check.column)
			}
		} else if columns[0].DataType != check.dataType || columns[0].IsNullable != "NO" || (check.defaultValue != "" && (columns[0].ColumnDefault == nil || normalizeSchemaDefinition(*columns[0].ColumnDefault) != normalizeSchemaDefinition(check.defaultValue))) {
			return fmt.Errorf("%w: %s.%s needs its canonical type, nullability and default", ErrManualMigrationRequired, check.table, check.column)
		}
	}
	var obsolete bool
	if err := db.Raw(`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND
	 ((table_name='gocommon_chat_channel_participants' AND column_name='is_muted') OR
	  (table_name='gocommon_chat_channels' AND column_name='support_replies') OR
	  (table_name IN ('gocommon_notifications','gocommon_notification_emissions') AND column_name IN ('payload_version','producer_version'))))`).Scan(&obsolete).Error; err != nil {
		return err
	}
	if obsolete {
		return fmt.Errorf("%w: obsolete or unsupported prerelease columns remain", ErrManualMigrationRequired)
	}
	for _, statement := range sharedRawSchemaStatements() {
		if err := validateSharedDDL(db, statement); err != nil {
			return err
		}
	}
	for _, constraint := range sharedOwnershipConstraints() {
		if err := validateSharedConstraint(db, constraint); err != nil {
			return err
		}
	}
	if err := requireSharedData(db); err != nil {
		return err
	}
	for _, check := range []struct{ table, predicate string }{
		{"gocommon_chat_messages", "kind NOT BETWEEN 1 AND 7 OR role NOT BETWEEN 1 AND 4"},
		// OAuth identities live in gocommon_oauth_logins keyed by provider slug;
		// legacy per-provider methods and token methods are moved by the SQL.
		{"gocommon_user_auth_methods", "method_type NOT IN (9,10)"},
		{"gocommon_auth_tokens", "auth_method BETWEEN 1 AND 8"},
		{"gocommon_attachments", "visibility NOT IN (1,2)"},
		{"gocommon_chat_agent_runs", "agent_session_id=0"},
		{"gocommon_chat_agent_sessions", "status NOT IN (1,2)"},
		{"gocommon_chat_channel_participants", "notification_mode NOT IN (1,2,3)"},
		{"gocommon_chat_messages", "kind=6 AND role=1 AND agent_note_subtype='' AND content IN ('You produced only internal reasoning with no visible reply. Respond with your answer to the user or call a tool.','Your previous response was empty. Respond with your answer to the user or call a tool.')"},
	} {
		var exists bool
		if err := db.Raw("SELECT to_regclass(format('%I.%I',current_schema(),?::text)) IS NOT NULL", check.table).Scan(&exists).Error; err != nil {
			return err
		}
		if !exists {
			continue
		}
		var invalid bool
		if err := db.Raw(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s)", check.table, check.predicate)).Scan(&invalid).Error; err != nil {
			return err
		}
		if invalid {
			return fmt.Errorf("%w: %s contains a representation requiring manual normalization", ErrManualMigrationRequired, check.table)
		}
	}
	return nil
}

// sharedRawSchemaStatements is also used by setup, so preflight and creation
// always agree about the definitions of the final shared objects.
func sharedRawSchemaStatements() []string {
	statements := append([]string{}, performanceIndexStatements()...)
	statements = append(statements, trigramIndexStatements()...)
	statements = append(statements, chatAgenticSchemaStatements()...)
	statements = append(statements, chatAgentInteractionSchemaStatements()...)
	statements = append(statements, inferenceControlPlaneIndexStatements()...)
	statements = append(statements, notificationIndexStatements()...)
	statements = append(statements, notificationFanoutIndexes...)
	statements = append(statements, notificationOwnSnapshotIndexStatements()...)
	statements = append(statements,
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_personal_owner_root_unique ON gocommon_organizations (personal_owner_user_id) WHERE parent_organization_id IS NULL AND personal_owner_user_id IS NOT NULL",
		"CREATE INDEX IF NOT EXISTS idx_user_organizations_user_org_covering ON gocommon_user_organizations (user_id, organization_id) INCLUDE (role, joined_at, updated_at)")
	return statements
}

var sharedIndexName = regexp.MustCompile(`(?i)^CREATE (?:UNIQUE )?INDEX IF NOT EXISTS \"?([a-z0-9_]+)\"? ON \"?([a-z0-9_]+)\"? `)
var ddlCasts = regexp.MustCompile(`::(?:text|smallint|integer|bigint|character varying)(?:\([0-9]+\))?(?:\[\])?`)
var ddlAny = regexp.MustCompile(`=\s*any\s*\(+\s*array\[([^\]]*)\]\s*\)+`)
var ddlBetween = regexp.MustCompile(`([a-z0-9_]+)\s+between\s+([0-9]+)\s+and\s+([0-9]+)`)
var ddlQuoted = regexp.MustCompile(`'(?:''|[^'])*'|"(?:""|[^"])*"`)
var ddlSimpleIdentifier = regexp.MustCompile(`^"[a-z_][a-z0-9_]*"$`)
var ddlSpace = regexp.MustCompile(`[\s"()]`)

// normalizeSchemaDefinition handles PostgreSQL's canonical rendering of the
// fixed shared DDL, including IN lists rendered as ANY arrays. It is deliberately
// conservative: differently defined same-named objects must be reviewed manually.
func normalizeSchemaDefinition(s string) string {
	var quoted []string
	s = ddlQuoted.ReplaceAllStringFunc(s, func(q string) string {
		// Lowercase identifiers used by the shared schema need no quoting; all
		// other identifiers and every string literal retain their exact bytes.
		if strings.HasPrefix(q, `"`) && ddlSimpleIdentifier.MatchString(q) {
			return strings.Trim(q, `"`)
		}
		quoted = append(quoted, q)
		return fmt.Sprintf("__quoted_%d__", len(quoted)-1)
	})
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "if not exists ", "")
	s = strings.ReplaceAll(s, " using btree", "")
	s = ddlCasts.ReplaceAllString(s, "")
	s = ddlAny.ReplaceAllString(s, " in ($1)")
	s = ddlBetween.ReplaceAllString(s, "$1 >= $2 and $1 <= $3")
	// Keep Boolean grouping significant; PostgreSQL only adds redundant wrappers.
	if where := strings.Index(s, " where "); where >= 0 {
		s = ddlSpace.ReplaceAllString(s[:where], "") + "where" + normalizeBooleanDefinition(s[where+7:])
	} else if strings.HasPrefix(strings.TrimSpace(s), "check") {
		s = "check" + normalizeBooleanDefinition(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "check")))
	} else {
		s = ddlSpace.ReplaceAllString(s, "")
	}
	for i, q := range quoted {
		s = strings.ReplaceAll(s, fmt.Sprintf("__quoted_%d__", i), q)
	}
	return s
}

func normalizeBooleanDefinition(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		depth := 0
		wraps := true
		for i, r := range s {
			if r == '(' {
				depth++
			}
			if r == ')' {
				depth--
			}
			if depth == 0 && i < len(s)-1 {
				wraps = false
				break
			}
		}
		if !wraps {
			break
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	for _, op := range []string{" or ", " and "} {
		depth := 0
		for i := 0; i < len(s); i++ {
			if s[i] == '(' {
				depth++
			}
			if s[i] == ')' {
				depth--
			}
			if depth == 0 && strings.HasPrefix(s[i:], op) {
				return "(" + normalizeBooleanDefinition(s[:i]) + strings.TrimSpace(op) + normalizeBooleanDefinition(s[i+len(op):]) + ")"
			}
		}
	}
	return ddlSpace.ReplaceAllString(s, "")
}

// executeSharedDDL validates final raw indexes instead of allowing IF NOT EXISTS
// to silently retain a conflicting definition. Never drops or replaces an index.
func validateSharedDDL(db *gorm.DB, statement string) error {
	if constraint, ok := parseSharedConstraint(statement); ok {
		return validateSharedConstraint(db, constraint)
	}
	match := sharedIndexName.FindStringSubmatch(statement)
	if match != nil {
		var indexes []struct {
			Definition string
			Valid      bool
		}
		if err := db.Raw(`SELECT COALESCE(pg_get_indexdef(i.indexrelid),format('%I (relation kind %s)',c.relname,c.relkind)) AS definition, COALESCE(i.indisvalid AND i.indisready AND i.indislive,false) AS valid
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_index i ON i.indexrelid=c.oid
WHERE n.nspname=current_schema() AND c.relname=?`, match[1]).Scan(&indexes).Error; err != nil {
			return err
		}
		if len(indexes) > 0 {
			var schema string
			if err := db.Raw("SELECT current_schema()").Scan(&schema).Error; err != nil {
				return err
			}
			actual := strings.ReplaceAll(indexes[0].Definition, fmt.Sprintf("%s.%s", schema, match[2]), match[2])
			actual = strings.ReplaceAll(actual, fmt.Sprintf(`"%s"."%s"`, schema, match[2]), match[2])
			if !indexes[0].Valid || normalizeSchemaDefinition(actual) != normalizeSchemaDefinition(statement) {
				return fmt.Errorf("%w: index %s has an incompatible definition (found %s; expected %s)", ErrManualMigrationRequired, match[1], normalizeSchemaDefinition(actual), normalizeSchemaDefinition(statement))
			}
			return nil
		}
	}
	return nil
}

func executeSharedDDL(db *gorm.DB, statement string) error {
	if constraint, ok := parseSharedConstraint(statement); ok {
		return executeSharedConstraint(db, constraint)
	}
	if err := validateSharedDDL(db, statement); err != nil {
		return err
	}
	return db.Exec(statement).Error
}
