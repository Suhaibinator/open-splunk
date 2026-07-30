package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
		profile := profile
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
		profile := profile
		for missingIndex := range profile.allowlist {
			missingIndex := missingIndex
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
		"GRANT unexpected_role TO runtime",
		"REVOKE INSERT ON open_splunk.events FROM runtime",
	}
	for _, profile := range testClickHousePrivilegeProfiles() {
		profile := profile
		for _, statement := range excessStatements {
			statement := statement
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
		test := test
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
	queries      []string
	nilGrantRows bool
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
		return fakeClickHousePrivilegeRow{
			err: fmt.Errorf("unexpected query %q", query),
		}
	}
}

type fakeClickHousePrivilegeRow struct {
	value string
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
	destination, ok := destinations[0].(*string)
	if !ok {
		return fmt.Errorf(
			"scan destination = %T, want *string",
			destinations[0],
		)
	}
	*destination = row.value
	return nil
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
