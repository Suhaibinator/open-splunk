package testsupport

import (
	"context"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartClickHouseRejectsNilContextWithoutCallingDocker(t *testing.T) {
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if _, err := StartClickHouse(nil, ""); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("StartClickHouse(nil) error = %v", err)
	}
}

func TestStartClickHouseWithServicePrincipalsRejectsNilContextWithoutCallingDocker(t *testing.T) {
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if _, err := StartClickHouseWithServicePrincipals(nil, ""); err == nil ||
		!strings.Contains(err.Error(), "context is required") {
		t.Fatalf("StartClickHouseWithServicePrincipals(nil) error = %v", err)
	}
}

func TestExecuteBootstrapSQLForTestRejectsInvalidInputWithoutCallingDocker(
	t *testing.T,
) {
	t.Parallel()

	if err := (*ClickHouseContainer)(nil).ExecuteBootstrapSQLForTest(
		context.Background(),
		"SELECT 1",
	); err == nil || !strings.Contains(err.Error(), "fixture is required") {
		t.Fatalf("nil fixture error = %v", err)
	}
	container := &ClickHouseContainer{
		bootstrapUsername: "bootstrap",
		bootstrapPassword: "secret",
	}
	if err := container.ExecuteBootstrapSQLForTest(
		context.Background(),
		" \t\n",
	); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("empty query error = %v", err)
	}
}

func TestServicePrincipalAccessConfigRequiresExplicitSystemDatabaseGrants(t *testing.T) {
	var config struct {
		AccessControlImprovements struct {
			SelectFromSystemDatabaseRequiresGrant bool `xml:"select_from_system_db_requires_grant"`
		} `xml:"access_control_improvements"`
	}
	if err := xml.Unmarshal([]byte(servicePrincipalAccessConfig), &config); err != nil {
		t.Fatalf("parse service-principal access config: %v", err)
	}
	if !config.AccessControlImprovements.SelectFromSystemDatabaseRequiresGrant {
		t.Fatal("select_from_system_db_requires_grant = false, want true")
	}
}

func TestServicePrincipalDockerArgumentsDoNotCreateSchema(t *testing.T) {
	container := &ClickHouseContainer{
		Name:  "fixture",
		Image: DefaultClickHouseImage,
	}
	arguments := servicePrincipalDockerArguments(
		container,
		"/tmp/access.xml",
		"bootstrap",
		strings.Repeat("a", 64),
	)
	joined := strings.Join(arguments, "\n")
	if strings.Contains(joined, "CLICKHOUSE_DB") {
		t.Fatalf("service-principal Docker arguments create schema:\n%s", joined)
	}
	for _, required := range []string{
		"CLICKHOUSE_USER=bootstrap",
		"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		"/etc/clickhouse-server/config.d/open-splunk-access.xml:ro",
		DefaultClickHouseImage,
	} {
		if !strings.Contains(joined, required) {
			t.Errorf("service-principal Docker arguments missing %q:\n%s", required, joined)
		}
	}
}

func TestServicePrincipalProvisioningSQLUsesOnlyScopedGrants(t *testing.T) {
	migrationPassword := strings.Repeat("a", 64)
	runtimePassword := strings.Repeat("b", 64)
	deletionPassword := strings.Repeat("c", 64)
	query, err := servicePrincipalProvisioningSQL(
		migrationPassword,
		runtimePassword,
		deletionPassword,
	)
	if err != nil {
		t.Fatalf("build service-principal provisioning SQL: %v", err)
	}
	if got := strings.Count(query, "CREATE ROLE IF NOT EXISTS "); got != 0 {
		t.Errorf("created role count = %d, want 0", got)
	}
	if got := strings.Count(query, "CREATE USER IF NOT EXISTS "); got != 3 {
		t.Errorf("created user count = %d, want 3", got)
	}
	for _, required := range []string{
		"CREATE USER IF NOT EXISTS open_splunk_migrator",
		"GRANT CREATE DATABASE ON open_splunk.* TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator",
		"GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX ON open_splunk.events TO open_splunk_migrator",
		"GRANT SELECT ON system.tables TO open_splunk_migrator",
		"GRANT SELECT, INSERT ON open_splunk.schema_migrations TO open_splunk_migrator",
		"CREATE USER IF NOT EXISTS open_splunk_runtime",
		"GRANT SELECT, INSERT ON open_splunk.events TO open_splunk_runtime",
		"GRANT SELECT(database, table, active, rows, bytes_on_disk) ON system.parts TO open_splunk_runtime",
		"CREATE USER IF NOT EXISTS open_splunk_deletion",
		"GRANT ALTER DELETE, SELECT(tenant_id, index_name) ON open_splunk.events TO open_splunk_deletion",
		"GRANT SELECT ON system.tables TO open_splunk_deletion",
		"GRANT SELECT ON system.mutations TO open_splunk_deletion",
	} {
		if !strings.Contains(query, required) {
			t.Errorf("service-principal provisioning SQL missing %q", required)
		}
	}
	upperQuery := strings.ToUpper(query)
	for _, forbidden := range []string{
		"GRANT ALL",
		"WITH GRANT OPTION",
		"WITH ADMIN OPTION",
		"CREATE ROLE ",
		"DEFAULT ROLE",
		" ON *.* ",
		"CREATE DATABASE OPEN_SPLUNK",
		"CREATE TABLE OPEN_SPLUNK.",
		"ALTER TABLE OPEN_SPLUNK.",
		"GRANT SELECT ON SYSTEM.PARTS TO OPEN_SPLUNK_RUNTIME",
		"GRANT SELECT ON SYSTEM.* TO OPEN_SPLUNK_RUNTIME",
		"OPEN_SPLUNK_BOOTSTRAP",
	} {
		if strings.Contains(upperQuery, forbidden) {
			t.Errorf("service-principal provisioning SQL contains forbidden %q", forbidden)
		}
	}
}

func TestServicePrincipalProvisioningSQLRejectsUnsafeCredentials(t *testing.T) {
	valid := strings.Repeat("a", 64)
	for name, password := range map[string]string{
		"short":     strings.Repeat("a", 63),
		"uppercase": strings.Repeat("A", 64),
		"quote":     strings.Repeat("a", 63) + "'",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := servicePrincipalProvisioningSQL(password, valid, valid); err == nil {
				t.Fatal("servicePrincipalProvisioningSQL() error = nil")
			}
		})
	}
}

func TestClickHouseContainerCloseRemovesServicePrincipalConfig(t *testing.T) {
	configDirectory, err := os.MkdirTemp(t.TempDir(), "clickhouse-config-")
	if err != nil {
		t.Fatalf("create config directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDirectory, "access.xml"), []byte("config"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	container := &ClickHouseContainer{
		configDirectory:   configDirectory,
		bootstrapUsername: "bootstrap",
		bootstrapPassword: "secret",
	}
	if err := container.Close(context.Background()); err != nil {
		t.Fatalf("close config-only container: %v", err)
	}
	if _, err := os.Stat(configDirectory); !os.IsNotExist(err) {
		t.Fatalf("config directory stat error = %v, want not exist", err)
	}
	if container.configDirectory != "" {
		t.Fatalf("configDirectory = %q, want empty", container.configDirectory)
	}
	if container.bootstrapUsername != "" || container.bootstrapPassword != "" {
		t.Fatal("bootstrap credentials were retained after close")
	}
}

func TestBoundedOutputRetainsDiagnosticTail(t *testing.T) {
	input := strings.Repeat("prefix", 1_000) + "diagnostic-tail\n"
	output := boundedOutput([]byte(input))
	if len(output) > 4<<10 || !strings.HasSuffix(output, "diagnostic-tail") {
		t.Fatalf("bounded output length = %d, suffix retained = %v", len(output), strings.HasSuffix(output, "diagnostic-tail"))
	}
}
