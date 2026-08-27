package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	clickhouserow "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var _ ClickHousePrivilegeConnection = (clickhousedriver.Conn)(nil)

func TestValidateClickHousePrincipalPrivilegesAcceptsExactAllowlists(t *testing.T) {
	t.Parallel()

	for _, profile := range testClickHousePrivilegeProfiles() {
		t.Run(profile.name, func(t *testing.T) {
			t.Parallel()

			connection := validClickHousePrivilegeConnection(profile.allowlist)
			if err := profile.validate(context.Background(), connection); err != nil {
				t.Fatalf("%s() error = %v", profile.validatorName, err)
			}
			if got, want := connection.queriesSnapshot(), []string{
				clickHouseVersionQuery,
				clickHouseExplicitGrantsQuery,
				clickHouseSystemGrantEnforcementCanary,
			}; !reflect.DeepEqual(got, want) {
				t.Fatalf("queries = %#v, want %#v", got, want)
			}
		})
	}
}

func TestValidateClickHouseApplicationPrivilegesAcceptsUnifiedAccount(t *testing.T) {
	t.Parallel()

	connection := validClickHouseApplicationPrivilegeConnection()
	connection.grants = []string{
		"GRANT ALL ON *.* TO application WITH GRANT OPTION",
	}
	if err := ValidateClickHouseApplicationPrivileges(
		context.Background(),
		connection,
	); err != nil {
		t.Fatalf("ValidateClickHouseApplicationPrivileges() error = %v", err)
	}
	wantQueries := []string{clickHouseVersionQuery}
	for _, grant := range clickHouseApplicationRequiredGrants {
		wantQueries = append(wantQueries, clickHouseGrantCheckQuery(grant))
	}
	if got := connection.queriesSnapshot(); !reflect.DeepEqual(got, wantQueries) {
		t.Fatalf("queries = %#v, want %#v", got, wantQueries)
	}
}

func TestValidateClickHouseApplicationPrivilegesRejectsEachMissingGrant(
	t *testing.T,
) {
	t.Parallel()

	for missingIndex, grant := range clickHouseApplicationRequiredGrants {
		t.Run(fmt.Sprintf("%d/%s", missingIndex, grant.target), func(t *testing.T) {
			t.Parallel()

			connection := validClickHouseApplicationPrivilegeConnection()
			connection.checkGrants[clickHouseGrantCheckQuery(grant)] = 0
			err := ValidateClickHouseApplicationPrivileges(
				context.Background(),
				connection,
			)
			if !errors.Is(err, ErrClickHousePrivilegeMissing) {
				t.Fatalf(
					"ValidateClickHouseApplicationPrivileges() error = %v, want missing grant",
					err,
				)
			}
		})
	}
}

func TestValidateClickHousePrincipalPrivilegesRejectsUnsupportedServerVersion(
	t *testing.T,
) {
	t.Parallel()

	connection := validClickHousePrivilegeConnection(
		clickHouseRuntimeGrantAllowlist,
	)
	connection.version = "26.4.1.1"
	err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	)
	if !errors.Is(err, ErrClickHouseVersionUnsupported) {
		t.Fatalf("validation error = %v, want unsupported version", err)
	}
	if got := connection.queriesSnapshot(); len(got) != 1 {
		t.Fatalf("queries after version failure = %#v, want one", got)
	}
}

func TestValidateClickHousePrincipalPrivilegesRejectsEachMissingGrant(t *testing.T) {
	t.Parallel()

	for _, profile := range testClickHousePrivilegeProfiles() {
		for missingIndex := range profile.allowlist {
			t.Run(fmt.Sprintf("%s/%d", profile.name, missingIndex), func(t *testing.T) {
				t.Parallel()

				grants := append(
					[]clickHouseGrant(nil),
					profile.allowlist[:missingIndex]...,
				)
				grants = append(grants, profile.allowlist[missingIndex+1:]...)
				connection := validClickHousePrivilegeConnection(grants)
				err := profile.validate(context.Background(), connection)
				if !errors.Is(err, ErrClickHousePrivilegeMissing) {
					t.Fatalf(
						"%s() error = %v, want ErrClickHousePrivilegeMissing",
						profile.validatorName,
						err,
					)
				}
			})
		}
	}
}

func TestValidateClickHousePrincipalPrivilegesRejectsAdversarialExcess(t *testing.T) {
	t.Parallel()

	excessStatements := []string{
		"GRANT ALTER UPDATE ON open_splunk.events TO runtime",
		"GRANT ALTER MOVE PARTITION ON open_splunk.events TO runtime",
		"GRANT ALTER TTL ON open_splunk.events TO runtime",
		"GRANT OPTIMIZE ON open_splunk.events TO runtime",
		"GRANT SYSTEM ON *.* TO runtime",
		"GRANT KILL QUERY ON *.* TO runtime",
		"GRANT SELECT ON open_splunk.* TO runtime",
		"GRANT SELECT ON open_splunk.events TO runtime WITH GRANT OPTION",
		"GRANT SELECT ON system.parts TO runtime",
		"GRANT SELECT(active, bytes_on_disk, database, name, rows, table) ON system.parts TO runtime",
		"GRANT SELECT ON system.columns TO runtime",
		"GRANT unexpected_role TO runtime",
		"REVOKE INSERT ON open_splunk.events FROM runtime",
	}
	for _, profile := range testClickHousePrivilegeProfiles() {
		for _, statement := range excessStatements {
			t.Run(profile.name+"/"+statement, func(t *testing.T) {
				t.Parallel()

				connection := validClickHousePrivilegeConnection(profile.allowlist)
				connection.grants = append(connection.grants, statement)
				err := profile.validate(context.Background(), connection)
				if !errors.Is(err, ErrClickHousePrivilegeProhibited) {
					t.Fatalf(
						"%s() error = %v, want ErrClickHousePrivilegeProhibited",
						profile.validatorName,
						err,
					)
				}
			})
		}
	}
}

func TestValidateClickHouseRuntimePrivilegesRequiresNarrowIndexStatisticsGrant(
	t *testing.T,
) {
	t.Parallel()

	want := []clickHouseGrant{
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
	if !reflect.DeepEqual(clickHouseRuntimeGrantAllowlist, want) {
		t.Fatalf(
			"runtime grant allowlist = %#v, want index-statistics least-privilege contract %#v",
			clickHouseRuntimeGrantAllowlist,
			want,
		)
	}

	connection := validClickHousePrivilegeConnection(want)
	if err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	); err != nil {
		t.Fatalf(
			"ValidateClickHouseRuntimePrivileges(index statistics) error = %v",
			err,
		)
	}
}

func TestValidateClickHouseMigrationPrivilegesRequiresRecoveryTableCreateGrants(
	t *testing.T,
) {
	t.Parallel()

	for _, target := range []string{
		"open_splunk.recovery_sets",
		"open_splunk.recovery_archive_markers",
	} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			connection := validClickHousePrivilegeConnection(
				clickHouseMigrationGrantAllowlist,
			)
			connection.grants = slicesWithoutClickHouseGrantTarget(
				connection.grants,
				target,
			)
			err := ValidateClickHouseMigrationPrivileges(
				context.Background(),
				connection,
			)
			if !errors.Is(err, ErrClickHousePrivilegeMissing) {
				t.Fatalf(
					"ValidateClickHouseMigrationPrivileges() error = %v, want missing %s CREATE TABLE grant",
					err,
					target,
				)
			}
		})
	}
}

func TestValidateClickHouseRecoveryPrivilegesUseSeparateLeastPrivilegeRoles(
	t *testing.T,
) {
	t.Parallel()

	if slicesContainClickHousePrivilege(
		clickHouseBackupGrantAllowlist,
		"CREATE DATABASE",
	) || slicesContainClickHousePrivilegeOutsideTarget(
		clickHouseBackupGrantAllowlist,
		"INSERT",
		"open_splunk.recovery_archive_markers",
	) {
		t.Fatalf("backup grant allowlist permits restore writes: %#v", clickHouseBackupGrantAllowlist)
	}
	if slicesContainClickHousePrivilege(
		clickHouseRestoreGrantAllowlist,
		"BACKUP",
	) {
		t.Fatalf("restore grant allowlist permits creating backups: %#v", clickHouseRestoreGrantAllowlist)
	}
	if slicesContainClickHousePrivilege(clickHouseRestoreGrantAllowlist, "DROP TABLE") {
		t.Fatalf("restore grant allowlist permits unnecessary table drops: %#v", clickHouseRestoreGrantAllowlist)
	}
	if slicesContainClickHousePrivilege(clickHouseRestoreGrantAllowlist, "DROP DATABASE") {
		t.Fatalf("restore grant allowlist permits unnecessary database drops: %#v", clickHouseRestoreGrantAllowlist)
	}
	for _, grant := range clickHouseRestoreGrantAllowlist {
		if strings.Contains(grant.target, "open_splunk*") {
			t.Fatalf("restore grant allowlist contains a prefix wildcard target: %#v", grant)
		}
		if grant.target == "open_splunk.*" &&
			!reflect.DeepEqual(grant.privileges, []string{"CREATE DATABASE", "SHOW TABLES"}) {
			t.Fatalf("restore database-wide grant exceeds creation and metadata visibility: %#v", grant)
		}
		for _, privilege := range grant.privileges {
			if (privilege == "CREATE TABLE" || privilege == "INSERT") &&
				grant.target != "open_splunk.events" &&
				grant.target != "open_splunk.schema_migrations" &&
				grant.target != "open_splunk.recovery_archive_markers" &&
				grant.target != "open_splunk.recovery_sets" {
				t.Fatalf("restore data mutation is not exact-table scoped: %#v", grant)
			}
		}
	}
	for name, profile := range map[string]struct {
		allowlist  []clickHouseGrant
		privileges []string
	}{
		"backup": {
			allowlist:  clickHouseBackupGrantAllowlist,
			privileges: []string{"BACKUP", "SHOW TABLES"},
		},
		"restore": {
			allowlist:  clickHouseRestoreGrantAllowlist,
			privileges: []string{"CREATE DATABASE", "SHOW TABLES"},
		},
	} {
		if !slicesContainExactClickHouseGrantPrivileges(
			profile.allowlist,
			"open_splunk.*",
			profile.privileges,
		) {
			t.Fatalf(
				"%s grant allowlist cannot observe administrator-owned schema additions: %#v",
				name,
				profile.allowlist,
			)
		}
	}
	if !slicesContainExactClickHouseGrant(
		clickHouseRestoreGrantAllowlist,
		"*.*",
		"SHOW DATABASES",
	) {
		t.Fatalf(
			"restore grant allowlist cannot observe the complete reserved namespace: %#v",
			clickHouseRestoreGrantAllowlist,
		)
	}
	if !slicesContainExactClickHouseGrant(
		clickHouseRestoreGrantAllowlist,
		"system.mutations",
		"SELECT",
	) {
		t.Fatalf(
			"restore grant allowlist cannot validate restored canonical mutations: %#v",
			clickHouseRestoreGrantAllowlist,
		)
	}
}

func TestValidateClickHouseRestorePrivilegesRejectsBroadEventReadsAndTruncates(
	t *testing.T,
) {
	t.Parallel()

	for _, statement := range []string{
		"GRANT SELECT ON open_splunk.events TO restore",
		"GRANT SELECT ON open_splunk_restore.events TO restore",
		"GRANT SELECT ON open_splunk*.events TO restore",
		"GRANT SELECT ON open_splunk*.events* TO restore",
		"GRANT TRUNCATE ON open_splunk.events TO restore",
		"GRANT TRUNCATE ON open_splunk_restore.events TO restore",
		"GRANT TRUNCATE ON open_splunk*.events TO restore",
		"GRANT TRUNCATE ON open_splunk*.events* TO restore",
		"GRANT TRUNCATE ON open_splunk.schema_migrations TO restore",
		"GRANT TRUNCATE ON open_splunk_restore.schema_migrations TO restore",
		"GRANT TRUNCATE ON open_splunk*.schema_migrations TO restore",
		"GRANT TRUNCATE ON open_splunk*.schema_migrations* TO restore",
	} {
		t.Run(statement, func(t *testing.T) {
			t.Parallel()

			connection := validClickHousePrivilegeConnection(
				clickHouseRestoreGrantAllowlist,
			)
			connection.grants = append(connection.grants, statement)
			err := ValidateClickHouseRestorePrivileges(
				context.Background(),
				connection,
			)
			if !errors.Is(err, ErrClickHousePrivilegeProhibited) {
				t.Fatalf(
					"ValidateClickHouseRestorePrivileges() error = %v, want prohibited broad grant",
					err,
				)
			}
		})
	}
}

func TestClickHouseRecoveryMarkerPrivilegesMatchOneShotResponsibilities(t *testing.T) {
	t.Parallel()

	if !slicesContainExactClickHouseGrantPrivileges(
		clickHouseBackupGrantAllowlist,
		"open_splunk.recovery_archive_markers",
		[]string{"INSERT", "SELECT", "TRUNCATE"},
	) {
		t.Fatalf(
			"backup grant allowlist cannot publish and clear canonical archive markers: %#v",
			clickHouseBackupGrantAllowlist,
		)
	}
	if !slicesContainExactClickHouseGrantPrivileges(
		clickHouseRestoreGrantAllowlist,
		"open_splunk.recovery_archive_markers",
		[]string{"CREATE TABLE", "INSERT", "SELECT", "TRUNCATE"},
	) {
		t.Fatalf(
			"restore grant allowlist cannot validate and consume the restored archive marker: %#v",
			clickHouseRestoreGrantAllowlist,
		)
	}
}

func slicesContainClickHousePrivilege(grants []clickHouseGrant, privilege string) bool {
	for _, grant := range grants {
		if slices.Contains(grant.privileges, privilege) {
			return true
		}
	}
	return false
}

func slicesContainClickHousePrivilegeOutsideTarget(
	grants []clickHouseGrant,
	privilege string,
	allowedTarget string,
) bool {
	for _, grant := range grants {
		if grant.target == allowedTarget {
			continue
		}
		if slices.Contains(grant.privileges, privilege) {
			return true
		}
	}
	return false
}

func slicesContainExactClickHouseGrant(
	grants []clickHouseGrant,
	target string,
	privilege string,
) bool {
	for _, grant := range grants {
		if grant.target == target && len(grant.privileges) == 1 &&
			grant.privileges[0] == privilege {
			return true
		}
	}
	return false
}

func slicesContainExactClickHouseGrantPrivileges(
	grants []clickHouseGrant,
	target string,
	privileges []string,
) bool {
	for _, grant := range grants {
		if grant.target == target && reflect.DeepEqual(grant.privileges, privileges) {
			return true
		}
	}
	return false
}

func slicesWithoutClickHouseGrantTarget(grants []string, target string) []string {
	filtered := make([]string, 0, len(grants))
	for _, grant := range grants {
		if !strings.Contains(grant, " ON "+target+" TO ") {
			filtered = append(filtered, grant)
		}
	}
	return filtered
}

func TestValidateClickHousePrincipalPrivilegesRejectsDuplicateGrant(t *testing.T) {
	t.Parallel()

	connection := validClickHousePrivilegeConnection(
		clickHouseRuntimeGrantAllowlist,
	)
	connection.grants = append(connection.grants, connection.grants[0])
	err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	)
	if !errors.Is(err, ErrClickHousePrivilegeProhibited) ||
		!strings.Contains(err.Error(), "repeats") {
		t.Fatalf("validation error = %v, want duplicate prohibited grant", err)
	}
}

func TestParseClickHouseGrantPreservesColumnScopedPrivilege(t *testing.T) {
	t.Parallel()

	grant, err := parseClickHouseGrant(
		"GRANT ALTER DELETE, SELECT(index_name, tenant_id) ON open_splunk.events TO deletion",
	)
	if err != nil {
		t.Fatalf("parseClickHouseGrant() error = %v", err)
	}
	if got, want := grant.privileges, []string{
		"ALTER DELETE",
		"SELECT(index_name, tenant_id)",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("privileges = %#v, want %#v", got, want)
	}
}

func TestParseClickHouseGrantRejectsMalformedPrivilegeLists(t *testing.T) {
	t.Parallel()

	for _, statement := range []string{
		"GRANT SELECT(index_name, tenant_id ON open_splunk.events TO deletion",
		"GRANT SELECT(index_name, tenant_id)) ON open_splunk.events TO deletion",
		"GRANT SELECT, , INSERT ON open_splunk.events TO runtime",
	} {
		if _, err := parseClickHouseGrant(statement); err == nil {
			t.Fatalf("parseClickHouseGrant(%q) error = nil", statement)
		}
	}
}

func TestValidateClickHousePrincipalPrivilegesRejectsDisabledSystemGrantEnforcement(
	t *testing.T,
) {
	t.Parallel()

	connection := validClickHousePrivilegeConnection(
		clickHouseRuntimeGrantAllowlist,
	)
	delete(
		connection.failures,
		clickHouseSystemGrantEnforcementCanary,
	)
	err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	)
	if !errors.Is(err, ErrClickHouseSystemGrantEnforcementDisabled) {
		t.Fatalf(
			"validation error = %v, want disabled enforcement",
			err,
		)
	}
}

func TestValidateClickHousePrincipalPrivilegesReportsUnexpectedCanaryErrors(
	t *testing.T,
) {
	t.Parallel()

	canaryFailure := &clickhousedriver.Exception{
		Code: 210,
		Name: "NETWORK_ERROR",
	}
	connection := validClickHousePrivilegeConnection(
		clickHouseRuntimeGrantAllowlist,
	)
	connection.failures[clickHouseSystemGrantEnforcementCanary] = canaryFailure
	err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	)
	if !errors.Is(err, canaryFailure) {
		t.Fatalf("validation error = %v, want canary failure", err)
	}
}

func TestValidateClickHousePrincipalPrivilegesReportsQueryErrors(t *testing.T) {
	t.Parallel()

	queryFailure := errors.New("ClickHouse unavailable")
	for _, query := range []string{
		clickHouseVersionQuery,
		clickHouseExplicitGrantsQuery,
	} {
		connection := validClickHousePrivilegeConnection(
			clickHouseRuntimeGrantAllowlist,
		)
		connection.failures[query] = queryFailure
		err := ValidateClickHouseRuntimePrivileges(
			context.Background(),
			connection,
		)
		if !errors.Is(err, queryFailure) {
			t.Fatalf(
				"validation error for %q = %v, want query failure",
				query,
				err,
			)
		}
	}
}

func TestValidateClickHousePrincipalPrivilegesRejectsNilGrantRows(t *testing.T) {
	t.Parallel()

	connection := validClickHousePrivilegeConnection(
		clickHouseRuntimeGrantAllowlist,
	)
	connection.nilGrantRows = true
	if err := ValidateClickHouseRuntimePrivileges(
		context.Background(),
		connection,
	); err == nil {
		t.Fatal("validation error = nil")
	}
}

func TestValidateClickHousePrincipalPrivilegesRequiresInputsAndLiveContext(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		ctx        context.Context
		connection ClickHousePrivilegeConnection
		wantError  error
	}{
		{
			name:       "nil context",
			connection: validClickHousePrivilegeConnection(clickHouseRuntimeGrantAllowlist),
		},
		{
			name: "nil connection",
			ctx:  context.Background(),
		},
		{
			name:       "canceled context",
			ctx:        canceledClickHousePrivilegeContext(),
			connection: validClickHousePrivilegeConnection(clickHouseRuntimeGrantAllowlist),
			wantError:  context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateClickHouseRuntimePrivileges(
				test.ctx,
				test.connection,
			)
			if err == nil {
				t.Fatal("validation error = nil, want input error")
			}
			if test.wantError != nil && !errors.Is(err, test.wantError) {
				t.Fatalf("validation error = %v, want %v", err, test.wantError)
			}
			if connection, ok := test.connection.(*fakeClickHousePrivilegeConnection); ok {
				if queries := connection.queriesSnapshot(); len(queries) != 0 {
					t.Fatalf("queries = %#v, want no query", queries)
				}
			}
		})
	}
}

type testClickHousePrivilegeProfile struct {
	name          string
	validatorName string
	validate      func(context.Context, ClickHousePrivilegeConnection) error
	allowlist     []clickHouseGrant
}

func testClickHousePrivilegeProfiles() []testClickHousePrivilegeProfile {
	return []testClickHousePrivilegeProfile{
		{
			name:          "migration",
			validatorName: "ValidateClickHouseMigrationPrivileges",
			validate:      ValidateClickHouseMigrationPrivileges,
			allowlist:     clickHouseMigrationGrantAllowlist,
		},
		{
			name:          "runtime",
			validatorName: "ValidateClickHouseRuntimePrivileges",
			validate:      ValidateClickHouseRuntimePrivileges,
			allowlist:     clickHouseRuntimeGrantAllowlist,
		},
		{
			name:          "deletion worker",
			validatorName: "ValidateClickHouseDeletionWorkerPrivileges",
			validate:      ValidateClickHouseDeletionWorkerPrivileges,
			allowlist:     clickHouseDeletionWorkerGrantAllowlist,
		},
		{
			name:          "backup",
			validatorName: "ValidateClickHouseBackupPrivileges",
			validate:      ValidateClickHouseBackupPrivileges,
			allowlist:     clickHouseBackupGrantAllowlist,
		},
		{
			name:          "restore",
			validatorName: "ValidateClickHouseRestorePrivileges",
			validate:      ValidateClickHouseRestorePrivileges,
			allowlist:     clickHouseRestoreGrantAllowlist,
		},
	}
}

func canceledClickHousePrivilegeContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type fakeClickHousePrivilegeConnection struct {
	mutex        sync.Mutex
	version      string
	grants       []string
	failures     map[string]error
	checkGrants  map[string]uint8
	queries      []string
	nilGrantRows bool
}

func validClickHouseApplicationPrivilegeConnection() *fakeClickHousePrivilegeConnection {
	connection := &fakeClickHousePrivilegeConnection{
		version:     clickHousePrivilegeContractVersion,
		failures:    make(map[string]error),
		checkGrants: make(map[string]uint8, len(clickHouseApplicationRequiredGrants)),
	}
	for _, grant := range clickHouseApplicationRequiredGrants {
		connection.checkGrants[clickHouseGrantCheckQuery(grant)] = 1
	}
	return connection
}

func validClickHousePrivilegeConnection(
	allowlist []clickHouseGrant,
) *fakeClickHousePrivilegeConnection {
	connection := &fakeClickHousePrivilegeConnection{
		version: clickHousePrivilegeContractVersion,
		failures: map[string]error{
			clickHouseSystemGrantEnforcementCanary: &clickhousedriver.Exception{
				Code: 497,
				Name: "ACCESS_DENIED",
			},
		},
	}
	for _, grant := range allowlist {
		connection.grants = append(
			connection.grants,
			formatClickHouseGrantStatement(grant, "principal"),
		)
	}
	return connection
}

func (connection *fakeClickHousePrivilegeConnection) Query(
	_ context.Context,
	query string,
	_ ...any,
) (clickhouserow.Rows, error) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.queries = append(connection.queries, query)
	if err, found := connection.failures[query]; found {
		return nil, err
	}
	switch query {
	case clickHouseExplicitGrantsQuery:
		if connection.nilGrantRows {
			return nil, nil
		}
		return &fakeClickHousePrivilegeRows{
			values: append([]string(nil), connection.grants...),
		}, nil
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
}

func (connection *fakeClickHousePrivilegeConnection) QueryRow(
	_ context.Context,
	query string,
	_ ...any,
) clickhouserow.Row {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.queries = append(connection.queries, query)
	if err, found := connection.failures[query]; found {
		return fakeClickHousePrivilegeRow{err: err}
	}
	switch query {
	case clickHouseVersionQuery:
		return fakeClickHousePrivilegeRow{value: connection.version}
	case clickHouseSystemGrantEnforcementCanary:
		return fakeClickHousePrivilegeRow{value: "access_control_path"}
	default:
		if granted, found := connection.checkGrants[query]; found {
			return fakeClickHousePrivilegeRow{value: granted}
		}
		return fakeClickHousePrivilegeRow{
			err: fmt.Errorf("unexpected query %q", query),
		}
	}
}

type fakeClickHousePrivilegeRow struct {
	value any
	err   error
}

func (row fakeClickHousePrivilegeRow) Err() error {
	return row.err
}

func (row fakeClickHousePrivilegeRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 1 {
		return fmt.Errorf(
			"scan destination count = %d, want 1",
			len(destinations),
		)
	}
	switch destination := destinations[0].(type) {
	case *string:
		value, ok := row.value.(string)
		if !ok {
			return fmt.Errorf("scan value = %T, want string", row.value)
		}
		*destination = value
		return nil
	case *uint8:
		value, ok := row.value.(uint8)
		if !ok {
			return fmt.Errorf("scan value = %T, want uint8", row.value)
		}
		*destination = value
		return nil
	default:
		return fmt.Errorf(
			"scan destination = %T, want *string or *uint8",
			destinations[0],
		)
	}
}

func (fakeClickHousePrivilegeRow) ScanStruct(any) error {
	return errors.New("ScanStruct is unsupported")
}

type fakeClickHousePrivilegeRows struct {
	values []string
	index  int
}

func (rows *fakeClickHousePrivilegeRows) Next() bool {
	if rows.index >= len(rows.values) {
		return false
	}
	rows.index++
	return true
}

func (rows *fakeClickHousePrivilegeRows) Scan(destinations ...any) error {
	if len(destinations) != 1 {
		return fmt.Errorf(
			"scan destination count = %d, want 1",
			len(destinations),
		)
	}
	destination, ok := destinations[0].(*string)
	if !ok {
		return fmt.Errorf(
			"scan destination = %T, want *string",
			destinations[0],
		)
	}
	if rows.index == 0 || rows.index > len(rows.values) {
		return errors.New("Scan called without a current row")
	}
	*destination = rows.values[rows.index-1]
	return nil
}

func (*fakeClickHousePrivilegeRows) ScanStruct(any) error {
	return errors.New("ScanStruct is unsupported")
}

func (*fakeClickHousePrivilegeRows) ColumnTypes() []clickhouserow.ColumnType {
	return nil
}

func (*fakeClickHousePrivilegeRows) Totals(...any) error {
	return nil
}

func (*fakeClickHousePrivilegeRows) Columns() []string {
	return []string{"GRANTS"}
}

func (*fakeClickHousePrivilegeRows) Close() error {
	return nil
}

func (*fakeClickHousePrivilegeRows) Err() error {
	return nil
}

func (rows *fakeClickHousePrivilegeRows) HasData() bool {
	return len(rows.values) != 0
}

func (connection *fakeClickHousePrivilegeConnection) queriesSnapshot() []string {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([]string(nil), connection.queries...)
}

func formatClickHouseGrantStatement(
	grant clickHouseGrant,
	grantee string,
) string {
	return fmt.Sprintf(
		"GRANT %s ON %s TO %s",
		strings.Join(grant.privileges, ", "),
		grant.target,
		grantee,
	)
}
