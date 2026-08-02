package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	// ErrClickHousePrivilegeMissing means a ClickHouse service principal cannot
	// perform an operation required by its assigned runtime contract.
	ErrClickHousePrivilegeMissing = errors.New("server: required ClickHouse privilege is missing")
	// ErrClickHousePrivilegeProhibited means a ClickHouse service principal has
	// an explicit grant outside its exact runtime allowlist.
	ErrClickHousePrivilegeProhibited = errors.New("server: prohibited ClickHouse privilege is granted")
	// ErrClickHouseSystemGrantEnforcementDisabled means ClickHouse still permits
	// historical implicit reads from non-public system tables. Runtime grant
	// validation is not meaningful unless the server requires explicit grants.
	ErrClickHouseSystemGrantEnforcementDisabled = errors.New(
		"server: ClickHouse system-table grant enforcement is disabled",
	)
	// ErrClickHouseVersionUnsupported means the server's implicit privilege
	// expansion may differ from the audited, integration-pinned contract.
	ErrClickHouseVersionUnsupported = errors.New(
		"server: ClickHouse server version is unsupported",
	)
)

const (
	clickHousePrivilegeContractVersion = "26.3.17.4"
	clickHouseVersionQuery             = "SELECT version()"
	clickHouseExplicitGrantsQuery      = "SHOW GRANTS"
	// system.tables is intentionally public even when ClickHouse requires
	// explicit system-database grants. system.server_settings is a stable
	// non-public canary: an exact runtime principal must receive ACCESS_DENIED.
	clickHouseSystemGrantEnforcementCanary = "SELECT name FROM system.server_settings LIMIT 1"
)

type clickHouseGrant struct {
	target     string
	privileges []string
}

var clickHouseRuntimeGrantAllowlist = []clickHouseGrant{
	{
		target:     "open_splunk.events",
		privileges: []string{"INSERT", "SELECT"},
	},
	{
		target: "system.parts",
		privileges: []string{
			"SELECT(active, bytes_on_disk, database, rows, `table`)",
		},
	},
}

var clickHouseMigrationGrantAllowlist = []clickHouseGrant{
	{
		target:     "open_splunk.*",
		privileges: []string{"CREATE DATABASE", "SHOW TABLES"},
	},
	{
		target: "open_splunk.events",
		privileges: []string{
			"ALTER ADD COLUMN",
			"ALTER ADD CONSTRAINT",
			"ALTER ADD INDEX",
			"CREATE TABLE",
		},
	},
	{
		target: "open_splunk.schema_migrations",
		privileges: []string{
			"CREATE TABLE",
			"INSERT",
			"SELECT",
		},
	},
	{
		target:     "open_splunk.recovery_sets",
		privileges: []string{"CREATE TABLE"},
	},
	{
		target:     "open_splunk.recovery_archive_markers",
		privileges: []string{"CREATE TABLE"},
	},
	{
		target:     "system.tables",
		privileges: []string{"SELECT"},
	},
}

var clickHouseDeletionWorkerGrantAllowlist = []clickHouseGrant{
	{
		target: "open_splunk.events",
		privileges: []string{
			"ALTER DELETE",
			"SELECT(index_name, tenant_id)",
		},
	},
	{
		target:     "system.mutations",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.tables",
		privileges: []string{"SELECT"},
	},
}

var clickHouseBackupGrantAllowlist = []clickHouseGrant{
	{
		target:     "open_splunk.*",
		privileges: []string{"BACKUP", "SHOW TABLES"},
	},
	{
		target:     "open_splunk.events",
		privileges: []string{"SELECT(visibility_seq)"},
	},
	{
		target:     "open_splunk.schema_migrations",
		privileges: []string{"SELECT"},
	},
	{
		target: "open_splunk.recovery_archive_markers",
		privileges: []string{
			"INSERT",
			"SELECT",
			"TRUNCATE",
		},
	},
	{
		target:     "system.databases",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.mutations",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.tables",
		privileges: []string{"SELECT"},
	},
}

var clickHouseRestoreGrantAllowlist = []clickHouseGrant{
	{
		target:     "open_splunk.*",
		privileges: []string{"CREATE DATABASE", "SHOW TABLES"},
	},
	{
		target: "open_splunk.events",
		privileges: []string{
			"CREATE TABLE",
			"INSERT",
			"SELECT(visibility_seq)",
		},
	},
	{
		target: "open_splunk.schema_migrations",
		privileges: []string{
			"CREATE TABLE",
			"INSERT",
			"SELECT(name, version)",
		},
	},
	{
		target: "open_splunk.recovery_archive_markers",
		privileges: []string{
			"CREATE TABLE",
			"INSERT",
			"SELECT",
			"TRUNCATE",
		},
	},
	{
		target: "open_splunk.recovery_sets",
		privileges: []string{
			"CREATE TABLE",
			"INSERT",
			"SELECT(database_uuid, deployment_manifest_sha256, events_table_uuid, recovery_archive_markers_table_uuid, recovery_set_id, recovery_sets_table_uuid, restored_at, schema_migrations_table_uuid, slot)",
			"TRUNCATE",
		},
	},
	{
		target:     "system.databases",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.disks",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.mutations",
		privileges: []string{"SELECT"},
	},
	{
		target:     "system.tables",
		privileges: []string{"SELECT"},
	},
	{
		target:     "*.*",
		privileges: []string{"SHOW DATABASES"},
	},
}

// ClickHouseVersionConnection is the subset of clickhouse-go's native
// connection needed to validate the server version.
type ClickHouseVersionConnection interface {
	QueryRow(ctx context.Context, query string, args ...any) clickhouserow.Row
}

// ClickHousePrivilegeConnection is the subset of clickhouse-go's native
// connection needed to validate a service principal. SHOW GRANTS replaces
// dozens of CHECK GRANT round trips with one complete explicit-grant read.
type ClickHousePrivilegeConnection interface {
	ClickHouseVersionConnection
	Query(ctx context.Context, query string, args ...any) (clickhouserow.Rows, error)
}

// ValidateClickHouseServerVersion pins implicit privilege expansion and SQL
// semantics before either migrations or a long-lived principal are used.
func ValidateClickHouseServerVersion(
	ctx context.Context,
	connection ClickHouseVersionConnection,
) error {
	if ctx == nil || connection == nil {
		return errors.New(
			"validate ClickHouse server version: context and connection are required",
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("validate ClickHouse server version: %w", err)
	}
	var version string
	if err := connection.QueryRow(
		ctx,
		clickHouseVersionQuery,
	).Scan(&version); err != nil {
		return fmt.Errorf("read ClickHouse server version: %w", err)
	}
	if version != clickHousePrivilegeContractVersion {
		return fmt.Errorf(
			"%w: connected to %q, want %q",
			ErrClickHouseVersionUnsupported,
			version,
			clickHousePrivilegeContractVersion,
		)
	}
	return nil
}

// ValidateClickHouseMigrationPrivileges verifies the short-lived schema
// principal before it can execute any migration DDL.
func ValidateClickHouseMigrationPrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) error {
	return validateClickHousePrincipalPrivileges(
		ctx,
		connection,
		"migration",
		clickHouseMigrationGrantAllowlist,
	)
}

// ValidateClickHouseRuntimePrivileges verifies the ordinary read/write
// principal. In addition to SELECT and INSERT on the events table, it may
// inspect only the active-part counters used by the bounded index-storage
// estimate.
func ValidateClickHouseRuntimePrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) error {
	return validateClickHousePrincipalPrivileges(
		ctx,
		connection,
		"runtime",
		clickHouseRuntimeGrantAllowlist,
	)
}

// ValidateClickHouseDeletionWorkerPrivileges verifies the isolated physical
// deletion principal. It can read only the event identity columns required by
// the zero proof. ALTER DELETE still permits destructive predicates and
// partition-drop operations outside the application's contract, so the worker
// connection remains private to the validated deletion Store.
func ValidateClickHouseDeletionWorkerPrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) error {
	return validateClickHousePrincipalPrivileges(
		ctx,
		connection,
		"deletion worker",
		clickHouseDeletionWorkerGrantAllowlist,
	)
}

// ValidateClickHouseBackupPrivileges verifies the stopped-server snapshot
// principal. In addition to inspecting release identity and quiescence, it can
// replace the canonical archive marker immediately around a native backup; it
// cannot create or restore objects.
func ValidateClickHouseBackupPrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) error {
	return validateClickHousePrincipalPrivileges(
		ctx,
		connection,
		"backup",
		clickHouseBackupGrantAllowlist,
	)
}

// ValidateClickHouseRestorePrivileges verifies the short-lived fresh-volume
// restore principal. Its DDL and INSERT access are limited to the exact
// canonical database; reads and truncates are further confined to recovery
// metadata plus the event visibility high-water mark. It cannot read raw event
// data, drop database objects, or create new backups.
func ValidateClickHouseRestorePrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) error {
	return validateClickHousePrincipalPrivileges(
		ctx,
		connection,
		"restore",
		clickHouseRestoreGrantAllowlist,
	)
}

func validateClickHousePrincipalPrivileges(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
	principal string,
	allowlist []clickHouseGrant,
) error {
	if ctx == nil || connection == nil {
		return fmt.Errorf(
			"validate ClickHouse %s privileges: context and connection are required",
			principal,
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("validate ClickHouse %s privileges: %w", principal, err)
	}
	if err := validateClickHousePrivilegeContractVersion(
		ctx,
		connection,
		principal,
	); err != nil {
		return err
	}

	statements, err := queryClickHouseGrantStatements(
		ctx,
		connection,
	)
	if err != nil {
		return fmt.Errorf(
			"read explicit ClickHouse %s grants: %w",
			principal,
			err,
		)
	}

	expected := make(map[string]struct{}, len(allowlist))
	for _, grant := range allowlist {
		expected[clickHouseGrantKey(grant.target, grant.privileges)] = struct{}{}
	}
	actual := make(map[string]string, len(statements))
	for _, statement := range statements {
		grant, err := parseClickHouseGrant(statement)
		if err != nil {
			return fmt.Errorf(
				"%w: %s principal has unexpected explicit grant %q: %w",
				ErrClickHousePrivilegeProhibited,
				principal,
				statement,
				err,
			)
		}
		key := clickHouseGrantKey(grant.target, grant.privileges)
		if previous, duplicated := actual[key]; duplicated {
			return fmt.Errorf(
				"%w: %s principal repeats explicit grant %q and %q",
				ErrClickHousePrivilegeProhibited,
				principal,
				previous,
				statement,
			)
		}
		actual[key] = statement
	}

	for _, key := range sortedClickHouseGrantKeys(actual) {
		if _, allowed := expected[key]; !allowed {
			return fmt.Errorf(
				"%w: %s principal has unexpected explicit grant %q",
				ErrClickHousePrivilegeProhibited,
				principal,
				actual[key],
			)
		}
	}
	for _, key := range sortedClickHouseGrantKeys(expected) {
		if _, granted := actual[key]; !granted {
			return fmt.Errorf(
				"%w: %s principal is missing %s",
				ErrClickHousePrivilegeMissing,
				principal,
				formatClickHouseGrantKey(key),
			)
		}
	}
	return validateClickHouseSystemGrantEnforcement(
		ctx,
		connection,
		principal,
	)
}

func validateClickHouseSystemGrantEnforcement(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
	principal string,
) error {
	var name string
	err := connection.QueryRow(
		ctx,
		clickHouseSystemGrantEnforcementCanary,
	).Scan(&name)
	if err == nil {
		return fmt.Errorf(
			"%w: %s principal could read system.server_settings",
			ErrClickHouseSystemGrantEnforcementDisabled,
			principal,
		)
	}
	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) && exception.Code == 497 {
		return nil
	}
	return fmt.Errorf(
		"probe ClickHouse %s system-table grant enforcement: %w",
		principal,
		err,
	)
}

func validateClickHousePrivilegeContractVersion(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
	principal string,
) error {
	if err := ValidateClickHouseServerVersion(
		ctx,
		connection,
	); err != nil {
		return fmt.Errorf(
			"validate ClickHouse %s privilege-contract version: %w",
			principal,
			err,
		)
	}
	return nil
}

func queryClickHouseGrantStatements(
	ctx context.Context,
	connection ClickHousePrivilegeConnection,
) (statements []string, returnErr error) {
	rows, err := connection.Query(ctx, clickHouseExplicitGrantsQuery)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, errors.New("SHOW GRANTS returned nil rows")
	}
	defer func() {
		returnErr = errors.Join(returnErr, rows.Close())
	}()
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return statements, nil
}

func parseClickHouseGrant(statement string) (clickHouseGrant, error) {
	statement = strings.TrimSpace(statement)
	const grantPrefix = "GRANT "
	if !strings.HasPrefix(statement, grantPrefix) {
		return clickHouseGrant{}, errors.New("statement is not a GRANT")
	}
	toIndex := strings.LastIndex(statement, " TO ")
	if toIndex < len(grantPrefix) || toIndex+len(" TO ") == len(statement) {
		return clickHouseGrant{}, errors.New("statement has no grantee")
	}
	grantee := strings.TrimSpace(statement[toIndex+len(" TO "):])
	if strings.Contains(grantee, " WITH GRANT OPTION") ||
		strings.Contains(grantee, " WITH ADMIN OPTION") {
		return clickHouseGrant{}, errors.New("grant or admin option is prohibited")
	}

	body := statement[len(grantPrefix):toIndex]
	onIndex := strings.LastIndex(body, " ON ")
	if onIndex <= 0 || onIndex+len(" ON ") == len(body) {
		return clickHouseGrant{}, errors.New("roles and grants without an ON target are prohibited")
	}
	target := strings.TrimSpace(body[onIndex+len(" ON "):])
	privilegeList := strings.TrimSpace(body[:onIndex])
	if target == "" || privilegeList == "" {
		return clickHouseGrant{}, errors.New("grant target and privileges are required")
	}
	privileges, err := splitClickHousePrivileges(privilegeList)
	if err != nil {
		return clickHouseGrant{}, err
	}
	return clickHouseGrant{target: target, privileges: privileges}, nil
}

func splitClickHousePrivileges(privilegeList string) ([]string, error) {
	var privileges []string
	start := 0
	parenthesisDepth := 0
	for index, character := range privilegeList {
		switch character {
		case '(':
			parenthesisDepth++
		case ')':
			parenthesisDepth--
			if parenthesisDepth < 0 {
				return nil, errors.New("unbalanced privilege column list")
			}
		case ',':
			if parenthesisDepth == 0 {
				privilege := strings.TrimSpace(privilegeList[start:index])
				if privilege == "" {
					return nil, errors.New("empty privilege is prohibited")
				}
				privileges = append(privileges, privilege)
				start = index + 1
			}
		}
	}
	if parenthesisDepth != 0 {
		return nil, errors.New("unbalanced privilege column list")
	}
	privilege := strings.TrimSpace(privilegeList[start:])
	if privilege == "" {
		return nil, errors.New("empty privilege is prohibited")
	}
	return append(privileges, privilege), nil
}

func clickHouseGrantKey(target string, privileges []string) string {
	ordered := append([]string(nil), privileges...)
	sort.Strings(ordered)
	return strings.TrimSpace(target) + "\x00" + strings.Join(ordered, "\x00")
}

func formatClickHouseGrantKey(key string) string {
	parts := strings.Split(key, "\x00")
	if len(parts) < 2 {
		return fmt.Sprintf("grant %q", key)
	}
	return fmt.Sprintf(
		"%s ON %s",
		strings.Join(parts[1:], ", "),
		parts[0],
	)
}

func sortedClickHouseGrantKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
