package testsupport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/xml"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
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

func TestStartSecureClickHouseWithServicePrincipalsRejectsInvalidInputsWithoutCallingDocker(
	t *testing.T,
) {
	//nolint:staticcheck // This case explicitly verifies the nil-context guard.
	if _, err := StartSecureClickHouseWithServicePrincipals(nil, ""); err == nil ||
		!strings.Contains(err.Error(), "context is required") {
		t.Fatalf("StartSecureClickHouseWithServicePrincipals(nil) error = %v", err)
	}
	if _, err := StartSecureClickHouseWithServicePrincipals(
		context.Background(),
		"clickhouse/clickhouse-server:latest",
	); err == nil || !strings.Contains(err.Error(), "digest-pinned") {
		t.Fatalf("mutable secure fixture image error = %v", err)
	}
}

func TestResolvePinnedClickHouseImage(t *testing.T) {
	t.Parallel()
	canonical := "registry.example/clickhouse:test@sha256:" + strings.Repeat("a1", 32)
	for name, test := range map[string]struct {
		image string
		want  string
		ok    bool
	}{
		"default":          {want: DefaultClickHouseImage, ok: true},
		"canonical":        {image: canonical, want: canonical, ok: true},
		"trimmed":          {image: " \t" + canonical + "\n", want: canonical, ok: true},
		"tag only":         {image: "clickhouse/clickhouse-server:latest"},
		"missing name":     {image: "@sha256:" + strings.Repeat("a", 64)},
		"short digest":     {image: "clickhouse@sha256:" + strings.Repeat("a", 63)},
		"uppercase digest": {image: "clickhouse@sha256:" + strings.Repeat("A", 64)},
		"trailing text":    {image: canonical + "-mutable"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolvePinnedClickHouseImage(test.image)
			if test.ok {
				if err != nil || got != test.want {
					t.Fatalf(
						"ResolvePinnedClickHouseImage(%q) = (%q, %v), want (%q, nil)",
						test.image,
						got,
						err,
						test.want,
					)
				}
				return
			}
			if err == nil || got != "" {
				t.Fatalf(
					"ResolvePinnedClickHouseImage(%q) = (%q, %v), want error",
					test.image,
					got,
					err,
				)
			}
		})
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

func TestSecureServicePrincipalDockerArgumentsExposeOnlyTLSNativePort(t *testing.T) {
	container := &ClickHouseContainer{
		Name:  "fixture",
		Image: DefaultClickHouseImage,
	}
	identity := &ServerTLSIdentity{
		CertificateFile: "/tmp/tls/server.crt",
		PrivateKeyFile:  "/tmp/tls/server.key",
	}
	arguments := secureServicePrincipalDockerArguments(
		container,
		"/tmp/access.xml",
		"/tmp/tls.xml",
		identity,
		"bootstrap",
		strings.Repeat("a", 64),
	)
	joined := strings.Join(arguments, "\n")
	for _, required := range []string{
		"127.0.0.1::9440",
		"/etc/clickhouse-server/config.d/open-splunk-access.xml:ro",
		"/etc/clickhouse-server/config.d/open-splunk-tls.xml:ro",
		"/etc/clickhouse-server/tls/server.crt:ro",
		"/etc/clickhouse-server/tls/server.key:ro",
		DefaultClickHouseImage,
	} {
		if !strings.Contains(joined, required) {
			t.Errorf("secure service-principal Docker arguments missing %q:\n%s", required, joined)
		}
	}
	for _, forbidden := range []string{
		"127.0.0.1::9000",
		"CLICKHOUSE_DB",
	} {
		if strings.Contains(joined, forbidden) {
			t.Errorf("secure service-principal Docker arguments contain %q:\n%s", forbidden, joined)
		}
	}
	for _, required := range []string{
		"<tcp_port_secure>9440</tcp_port_secure>",
		"<verificationMode>none</verificationMode>",
		"tlsv1,tlsv1_1",
	} {
		if !strings.Contains(secureClickHouseTLSConfig, required) {
			t.Errorf("secure ClickHouse TLS config missing %q", required)
		}
	}
}

func TestPrepareSecureClickHouseIdentityFilesAreReadableAfterPrivilegeDrop(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}

	directory := filepath.Join(t.TempDir(), "tls")
	identity, err := WriteServerTLSIdentity(directory, secureClickHouseTLSServerName)
	if err != nil {
		t.Fatalf("WriteServerTLSIdentity(): %v", err)
	}
	if err := prepareSecureClickHouseIdentityFiles(identity); err != nil {
		t.Fatalf("prepareSecureClickHouseIdentityFiles(): %v", err)
	}
	for name, path := range map[string]string{
		"certificate": identity.CertificateFile,
		"private key": identity.PrivateKeyFile,
	} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatalf("stat TLS %s: %v", name, statErr)
		}
		if got := info.Mode().Perm(); got != 0o644 {
			t.Fatalf("TLS %s mode = %#o, want 0644", name, got)
		}
	}
	directoryInfo, err := os.Stat(directory)
	if err != nil {
		t.Fatalf("stat TLS directory: %v", err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("TLS directory mode = %#o, want 0700", got)
	}
}

func TestSecureClickHouseReadinessOptionsUsePublishedTLSMigrationPrincipal(t *testing.T) {
	t.Parallel()

	roots := x509.NewCertPool()
	container := &ClickHouseContainer{
		Address:           " 127.0.0.1:19440 ",
		MigrationUsername: " open_splunk_migrator ",
		MigrationPassword: strings.Repeat("a", 64),
		TLSServerName:     " clickhouse.test ",
	}
	options, err := secureClickHouseReadinessOptions(
		container,
		&ServerTLSIdentity{RootCAs: roots},
	)
	if err != nil {
		t.Fatalf("secureClickHouseReadinessOptions(): %v", err)
	}
	if options.Protocol != clickhousedriver.Native ||
		len(options.Addr) != 1 || options.Addr[0] != "127.0.0.1:19440" ||
		options.Auth.Database != "default" ||
		options.Auth.Username != "open_splunk_migrator" ||
		options.Auth.Password != container.MigrationPassword {
		t.Fatalf("secure readiness options = %#v", options)
	}
	if options.TLS == nil || options.TLS.MinVersion != tls.VersionTLS12 ||
		options.TLS.ServerName != "clickhouse.test" ||
		options.TLS.RootCAs == nil || options.TLS.RootCAs == roots ||
		options.TLS.InsecureSkipVerify {
		t.Fatalf("secure readiness TLS config = %#v", options.TLS)
	}
	if options.DialTimeout != 5*time.Second ||
		options.ReadTimeout != 5*time.Second ||
		options.MaxOpenConns != 1 || options.MaxIdleConns != 1 {
		t.Fatalf("secure readiness bounds = %#v", options)
	}
}

func TestSecureClickHouseReadinessOptionsRejectIncompleteFixture(t *testing.T) {
	t.Parallel()

	valid := &ClickHouseContainer{
		Address:           "127.0.0.1:19440",
		MigrationUsername: "open_splunk_migrator",
		MigrationPassword: strings.Repeat("a", 64),
		TLSServerName:     "clickhouse.test",
	}
	identity := &ServerTLSIdentity{RootCAs: x509.NewCertPool()}
	for name, test := range map[string]struct {
		container *ClickHouseContainer
		identity  *ServerTLSIdentity
	}{
		"nil container": {identity: identity},
		"nil identity":  {container: valid},
		"nil roots": {
			container: valid,
			identity:  &ServerTLSIdentity{},
		},
		"empty endpoint": {
			container: &ClickHouseContainer{
				MigrationUsername: valid.MigrationUsername,
				MigrationPassword: valid.MigrationPassword,
				TLSServerName:     valid.TLSServerName,
			},
			identity: identity,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if options, err := secureClickHouseReadinessOptions(
				test.container,
				test.identity,
			); err == nil || options != nil {
				t.Fatalf("secure readiness options = (%#v, %v), want nil/error", options, err)
			}
		})
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
		"GRANT SHOW TABLES ON open_splunk.* TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.recovery_sets TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.recovery_archive_markers TO open_splunk_migrator",
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

func TestClickHouseContainerRemovalArgumentsIncludeAnonymousVolumes(t *testing.T) {
	t.Parallel()

	arguments := clickHouseContainerRemovalArguments("fixture")
	if got, want := strings.Join(arguments, " "),
		"rm --force --volumes fixture"; got != want {
		t.Fatalf("ClickHouse removal arguments = %q, want %q", got, want)
	}
}

func TestBoundedOutputRetainsDiagnosticTail(t *testing.T) {
	input := strings.Repeat("prefix", 1_000) + "diagnostic-tail\n"
	output := boundedOutput([]byte(input))
	if len(output) > 4<<10 || !strings.HasSuffix(output, "diagnostic-tail") {
		t.Fatalf("bounded output length = %d, suffix retained = %v", len(output), strings.HasSuffix(output, "diagnostic-tail"))
	}
}
