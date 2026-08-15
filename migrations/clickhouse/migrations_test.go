package clickhouse_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

var migrationNamePattern = regexp.MustCompile(`^(\d{4})_[a-z0-9]+(?:_[a-z0-9]+)*\.sql$`)

func TestMigrationFilesAreContiguous(t *testing.T) {
	files := migrationFiles(t)
	if len(files) == 0 {
		t.Fatal("no ClickHouse migrations found")
	}

	for i, path := range files {
		match := migrationNamePattern.FindStringSubmatch(filepath.Base(path))
		if match == nil {
			t.Fatalf("migration %q must match %s", filepath.Base(path), migrationNamePattern)
		}

		version, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("parse migration version in %q: %v", path, err)
		}
		want := i + 1
		if version != want {
			t.Fatalf("migration %q has version %d; want contiguous version %d", path, version, want)
		}

		sql := readFile(t, path)
		if strings.TrimSpace(sql) == "" {
			t.Fatalf("migration %q is empty", path)
		}
		if regexp.MustCompile(`(?im)^\s*(DROP|TRUNCATE)\b`).MatchString(sql) {
			t.Fatalf("migration %q contains destructive DDL", path)
		}
	}
}

func TestInitialEventsSchemaContract(t *testing.T) {
	sql := readFile(t, "0001_create_events.sql")

	for _, column := range []string{
		"event_id", "tenant_id", "index_name", "event_time", "index_time",
		"host", "source", "sourcetype", "service", "level", "body", "raw",
		"trace_id", "span_id", "fields", "field_names", "collector_id", "batch_id",
	} {
		if !strings.Contains(sql, "`"+column+"`") {
			t.Errorf("initial events migration is missing canonical column %q", column)
		}
	}

	for _, fragment := range []string{
		"`fields` JSON(",
		"max_dynamic_paths = 256",
		"max_dynamic_types = 16",
		"PARTITION BY toYYYYMM(`event_time`)",
		"PRIMARY KEY (`tenant_id`, `index_name`, toStartOfHour(`event_time`), `event_time`)",
		"ORDER BY (`tenant_id`, `index_name`, toStartOfHour(`event_time`), `event_time`, `event_id`)",
		"TTL `expires_at` DELETE",
		"non_replicated_deduplication_window = 10000",
		"INDEX idx_raw_text arrayMap(token -> lower(token), extractAll(translateUTF8(`raw`, 'ſK', 'sk'), '[A-Za-z0-9_]+')) TYPE text(tokenizer = 'array')",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("initial events migration is missing schema contract fragment %q", fragment)
		}
	}
}

func TestVisibilitySequenceMigrationContract(t *testing.T) {
	t.Parallel()
	sql := readFile(t, "0002_add_visibility_sequence.sql")
	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS `visibility_seq` UInt64 DEFAULT 0",
		"ADD CONSTRAINT IF NOT EXISTS visibility_seq_is_positive",
		"CHECK `visibility_seq` > 0",
		"ADD INDEX IF NOT EXISTS idx_visibility_seq `visibility_seq` TYPE minmax GRANULARITY 1",
		"SELECT 2, 'add_visibility_sequence'",
		"WHERE `version` = 2",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("visibility migration is missing contract fragment %q", fragment)
		}
	}
}

func TestFieldMetadataMigrationContract(t *testing.T) {
	t.Parallel()
	sql := readFile(t, "0003_add_field_metadata.sql")
	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS `field_types` Array(UInt8) DEFAULT []",
		"AFTER `field_names`",
		"ADD COLUMN IF NOT EXISTS `field_metadata_version` UInt8 DEFAULT 0",
		"AFTER `field_types`",
		"ADD CONSTRAINT IF NOT EXISTS field_metadata_version_is_supported",
		"CHECK `field_metadata_version` IN (0, 1)",
		"ADD CONSTRAINT IF NOT EXISTS field_metadata_is_aligned",
		"length(`field_names`) = length(`field_types`)",
		"arrayAll(code -> code BETWEEN 1 AND 12, `field_types`)",
		"SELECT 3, 'add_field_metadata'",
		"WHERE `version` = 3",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("field metadata migration is missing contract fragment %q", fragment)
		}
	}
}

func TestRecoverySetsMigrationContract(t *testing.T) {
	t.Parallel()
	sql := readFile(t, "0004_create_recovery_sets.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS open_splunk.recovery_sets",
		"`slot` UInt8",
		"`recovery_set_id` FixedString(32)",
		"`deployment_manifest_sha256` FixedString(64)",
		"`database_uuid` UUID",
		"`schema_migrations_table_uuid` UUID",
		"`events_table_uuid` UUID",
		"`recovery_sets_table_uuid` UUID",
		"`recovery_archive_markers_table_uuid` UUID",
		"`restored_at` DateTime64(3, 'UTC')",
		"CREATE TABLE IF NOT EXISTS open_splunk.recovery_archive_markers",
		"`backup_operation_uuid` UUID",
		"CONSTRAINT slot_is_singleton CHECK `slot` = 1",
		"ENGINE = MergeTree",
		"ORDER BY (`slot`)",
		"SELECT 4, 'create_recovery_sets'",
		"WHERE `version` = 4",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("recovery sets migration is missing contract fragment %q", fragment)
		}
	}
}

func TestIngestSourceMigrationContract(t *testing.T) {
	t.Parallel()
	sql := readFile(t, "0005_add_ingest_source.sql")
	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS `ingest_source_kind` UInt8",
		"DEFAULT if(empty(`collector_id`), 0, 1)",
		"ADD COLUMN IF NOT EXISTS `ingest_source_id` String",
		"DEFAULT if(`ingest_source_kind` = 1, `collector_id`, '')",
		"ADD CONSTRAINT IF NOT EXISTS ingest_source_kind_is_supported",
		"CHECK `ingest_source_kind` IN (1, 2)",
		"ADD CONSTRAINT IF NOT EXISTS ingest_source_shape_is_valid",
		"`ingest_source_id` = `collector_id`",
		"empty(`collector_id`)",
		"SELECT 5, 'add_ingest_source'",
		"WHERE `version` = 5",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("ingest source migration is missing contract fragment %q", fragment)
		}
	}
}

func TestComposeFullStackSecurityContract(t *testing.T) {
	compose := readFile(t, filepath.Join("..", "..", "deploy", "docker-compose.yaml"))

	for _, fragment := range []string{
		"image: " + testsupport.DefaultClickHouseImage,
		"user: \"101:101\"",
		"read_only: true",
		"cap_drop:",
		"- ALL",
		"no-new-privileges:true",
		"/tmp:rw,noexec,nosuid,nodev,mode=0700,uid=101,gid=101",
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD:?",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD:?",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD:?",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD:?",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME:?",
		"./clickhouse-init.sh:/docker-entrypoint-initdb.d/0100_open_splunk.sh:ro",
		"./clickhouse-config/access.xml:/etc/clickhouse-server/config.d/open_splunk_access.xml:ro",
		"./clickhouse-config/bootstrap-user.xml:/etc/clickhouse-server/users.d/open_splunk_bootstrap.xml:ro",
		"./clickhouse-config/tls.xml:/etc/clickhouse-server/config.d/open_splunk_tls.xml:ro",
		"./clickhouse-config/client-tls.xml:/etc/clickhouse-client/open-splunk-tls.xml:ro",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE:?",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE:?",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE:?",
		"CLICKHOUSE_SKIP_USER_SETUP: \"1\"",
		"CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS: \"1\"",
		"healthcheck:",
		"--config-file /etc/clickhouse-client/open-splunk-tls.xml",
		"--secure",
		"--host 127.0.0.1",
		"--port 9440",
		"--tls-sni-override",
		"CLICKHOUSE_PASSWORD=\"$${OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD}\"",
		"clickhouse-migrator:",
		"migrate-clickhouse",
		"/run/open-splunk/clickhouse/migration.password",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE:?",
		"server-bootstrap:",
		"user: \"65532:65532\"",
		"network_mode: none",
		"provision-administrator-token",
		"/var/lib/open-splunk/state/private/administrator.token",
		"server:",
		"condition: service_completed_successfully",
		"condition: service_healthy",
		"127.0.0.1:${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}:8080",
		"127.0.0.1:${OPEN_SPLUNK_SERVER_GRPC_PORT:-4317}:4317",
		"-http-address",
		"0.0.0.0:8080",
		"-collector-grpc-address",
		"0.0.0.0:4317",
		"-clickhouse-address",
		"clickhouse:9440",
		"-clickhouse-secure",
		"-clickhouse-server-name",
		"-clickhouse-skip-migrations",
		"-clickhouse-runtime-password-file",
		"/run/open-splunk/clickhouse/runtime.password",
		"-clickhouse-deletion-password-file",
		"/run/open-splunk/clickhouse/deletion.password",
		"/var/lib/open-splunk/state/private/open-splunk.db",
		"/var/lib/open-splunk/state/private/master.key",
		"/var/lib/open-splunk/exports/private",
		"/run/open-splunk/tls/server.crt",
		"/run/open-splunk/tls/server.key",
		"/run/open-splunk/tls/ca.crt",
		"healthcheck",
		"https://127.0.0.1:8080/readyz",
		"server-state:/var/lib/open-splunk/state",
		"server-lock:/var/lib/open-splunk/lock",
		"server-exports:/var/lib/open-splunk/exports",
		"internal: true",
		"TMPDIR: /tmp",
		"OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH: /var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock",
	} {
		if !strings.Contains(compose, fragment) {
			t.Errorf("deployment compose file is missing safety contract fragment %q", fragment)
		}
	}
	if strings.Contains(compose, ":latest") {
		t.Error("deployment compose file must not use a floating image")
	}
	if regexp.MustCompile(`(?m)^[ \t]+build[ \t]*:`).MatchString(compose) {
		t.Error("deployment Compose must use only images published by make oci")
	}
	if strings.Contains(compose, "0.0.0.0:${") {
		t.Error("deployment compose file must not use a wildcard host publication")
	}
	if strings.Contains(compose, "CLICKHOUSE_DB:") ||
		strings.Contains(compose, "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:") ||
		strings.Contains(compose, "../migrations/clickhouse") ||
		strings.Contains(compose, "/open-splunk-migrations") {
		t.Error("bootstrap principal must not create or migrate the application schema")
	}
	if strings.Contains(compose, "OPEN_SPLUNK_CLICKHOUSE_HTTP_PORT") ||
		strings.Contains(compose, "OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT") ||
		strings.Contains(compose, "OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT") {
		t.Error("ClickHouse ports must not be published by the full-stack deployment")
	}
	if count := strings.Count(compose, "OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD"); count != 1 {
		t.Fatalf("bootstrap password references = %d, want only the ClickHouse service", count)
	}
	migratorStart := strings.Index(compose, "\n  clickhouse-migrator:\n")
	bootstrapStart := strings.Index(compose, "\n  server-bootstrap:\n")
	if migratorStart < 0 || bootstrapStart <= migratorStart {
		t.Fatal("deployment compose file has no bounded one-shot migrator service")
	}
	migratorSection := compose[migratorStart:bootstrapStart]
	for _, fragment := range []string{
		"*open-splunk-server-image",
		"*open-splunk-server-security",
		"condition: service_healthy",
		"restart: \"no\"",
		"migrate-clickhouse",
		"clickhouse:9440",
		"/run/open-splunk/clickhouse/ca.crt:ro",
		"/run/open-splunk/clickhouse/migration.password:ro",
		"      - backend",
		"pids_limit: 64",
	} {
		if !strings.Contains(migratorSection, fragment) {
			t.Errorf("one-shot migrator is missing safety contract fragment %q", fragment)
		}
	}
	if strings.Contains(migratorSection, "\n    ports:") ||
		strings.Contains(migratorSection, "server-state:") ||
		strings.Contains(migratorSection, "runtime.password") ||
		strings.Contains(migratorSection, "deletion.password") ||
		strings.Contains(migratorSection, "      - frontend") {
		t.Error("one-shot migrator has ports, durable state, or non-migration credentials")
	}
	serverStart := strings.Index(compose, "\n  server:\n")
	if serverStart < 0 {
		t.Fatal("deployment compose file has no server service")
	}
	recoveryStart := strings.Index(compose, "\n  deployment-backup:\n")
	if recoveryStart <= bootstrapStart || recoveryStart >= serverStart {
		t.Fatal("deployment compose file has no bounded recovery-profile services")
	}
	bootstrapSection := compose[bootstrapStart:recoveryStart]
	for _, fragment := range []string{
		"*open-splunk-server-security",
		"network_mode: none",
		"-destination\n      - /var/lib/open-splunk/state/private/administrator.token",
		"- server-state:/var/lib/open-splunk/state",
	} {
		if !strings.Contains(bootstrapSection, fragment) {
			t.Errorf("administrator bootstrap is missing non-root volume contract fragment %q", fragment)
		}
	}
	if strings.Contains(bootstrapSection, "\n    user:") ||
		strings.Contains(bootstrapSection, "privileged: true") ||
		strings.Contains(bootstrapSection, "volume:\n          nocopy: true") {
		t.Error("administrator bootstrap overrides its fixed non-root identity or disables image-seeded volume population")
	}
	serverSection := compose[serverStart:]
	for _, fragment := range []string{
		"clickhouse-migrator:\n        condition: service_completed_successfully",
		"-clickhouse-skip-migrations",
		"-control-db\n      - /var/lib/open-splunk/state/private/open-splunk.db",
		"-master-key\n      - /var/lib/open-splunk/state/private/master.key",
		"-administrator-token-file\n      - /var/lib/open-splunk/state/private/administrator.token",
		"-export-artifact-dir\n      - /var/lib/open-splunk/exports/private",
		"- server-state:/var/lib/open-splunk/state",
		"- server-lock:/var/lib/open-splunk/lock",
		"- server-exports:/var/lib/open-splunk/exports",
	} {
		if !strings.Contains(serverSection, fragment) {
			t.Errorf("long-running server is missing migration boundary fragment %q", fragment)
		}
	}
	const portsStartMarker = "\n    ports:\n"
	if count := strings.Count(serverSection, portsStartMarker); count != 1 {
		t.Fatalf("server ports blocks = %d, want exactly one", count)
	}
	portsStart := strings.Index(serverSection, portsStartMarker)
	portsEndOffset := strings.Index(serverSection[portsStart+len(portsStartMarker):], "\n    volumes:\n")
	if portsEndOffset < 0 {
		t.Fatal("server ports block is not followed by its volumes block")
	}
	portsEnd := portsStart + len(portsStartMarker) + portsEndOffset
	gotPortsBlock := serverSection[portsStart:portsEnd]
	const wantPortsBlock = `
    ports:
      - "127.0.0.1:${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}:8080"
      - "127.0.0.1:${OPEN_SPLUNK_SERVER_GRPC_PORT:-4317}:4317"`
	if gotPortsBlock != wantPortsBlock {
		t.Fatalf(
			"server ports block = %q, want exactly %q",
			gotPortsBlock,
			wantPortsBlock,
		)
	}
	for _, name := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
	} {
		if strings.Contains(serverSection, name+`: "${`) {
			t.Errorf("server service exposes %s through its environment", name)
		}
		if !strings.Contains(serverSection, name+"_FILE:?") {
			t.Errorf("server service does not mount generated %s_FILE", name)
		}
	}
	if strings.Contains(serverSection, "OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD") ||
		strings.Contains(serverSection, "/run/open-splunk/clickhouse/migration.password") {
		t.Error("long-running server retains the short-lived migration credential")
	}
	if strings.Contains(compose, "\n  collector:") {
		t.Error("the default deployment must not run a collector before explicit onboarding")
	}
}

func deploymentDirectory(t *testing.T) string {
	t.Helper()
	directory, err := filepath.Abs(filepath.Join("..", "..", "deploy"))
	if err != nil {
		t.Fatalf("resolve deployment directory: %v", err)
	}
	return directory
}

func newGenerateEnvCommand(
	ctx context.Context,
	deployDirectory string,
	envFile string,
) *exec.Cmd {
	command := exec.CommandContext(
		ctx,
		"sh",
		filepath.Join(deployDirectory, "generate-env.sh"),
		envFile,
	)
	command.WaitDelay = 5 * time.Second
	return command
}

func mustGenerateDeploymentEnvironment(
	t *testing.T,
	ctx context.Context,
	deployDirectory string,
	envFile string,
) map[string]string {
	t.Helper()
	output, err := newGenerateEnvCommand(
		ctx,
		deployDirectory,
		envFile,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("generate deployment environment: %v: %s", err, output)
	}
	return parseDeploymentEnvironment(t, readFile(t, envFile))
}

func TestGenerateEnvAcceptsUTCCommitTimestamp(t *testing.T) {
	for _, dependency := range []string{"git", "openssl"} {
		if _, err := exec.LookPath(dependency); err != nil {
			t.Skipf("%s is unavailable: %v", dependency, err)
		}
	}

	repository := t.TempDir()
	deployDirectory := filepath.Join(repository, "deploy")
	if err := os.Mkdir(deployDirectory, 0o755); err != nil {
		t.Fatalf("create temporary deployment directory: %v", err)
	}
	generatorPath := filepath.Join(deployDirectory, "generate-env.sh")
	generator, err := os.ReadFile(filepath.Join(deploymentDirectory(t), "generate-env.sh"))
	if err != nil {
		t.Fatalf("read deployment environment generator: %v", err)
	}
	if err := os.WriteFile(generatorPath, generator, 0o755); err != nil {
		t.Fatalf("copy deployment environment generator: %v", err)
	}

	commandContext, cancelCommand := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelCommand()

	for _, arguments := range [][]string{
		{"init", "--quiet"},
		{"add", "deploy/generate-env.sh"},
	} {
		command := exec.CommandContext(commandContext, "git", arguments...)
		command.Dir = repository
		if output, commandErr := command.CombinedOutput(); commandErr != nil {
			t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), commandErr, output)
		}
	}
	commit := exec.CommandContext(
		commandContext,
		"git",
		"-c",
		"commit.gpgsign=false",
		"commit",
		"--quiet",
		"-m",
		"UTC timestamp",
	)
	commit.Dir = repository
	commit.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Open Splunk Test",
		"GIT_AUTHOR_EMAIL=open-splunk-test@example.com",
		"GIT_AUTHOR_DATE=2026-08-11T05:29:10+00:00",
		"GIT_COMMITTER_NAME=Open Splunk Test",
		"GIT_COMMITTER_EMAIL=open-splunk-test@example.com",
		"GIT_COMMITTER_DATE=2026-08-11T05:29:10+00:00",
	)
	if output, err := commit.CombinedOutput(); err != nil {
		t.Fatalf("commit UTC fixture: %v: %s", err, output)
	}

	envFile := filepath.Join(repository, "deployment.env")
	values := mustGenerateDeploymentEnvironment(
		t,
		commandContext,
		deployDirectory,
		envFile,
	)
	if got := values["OPEN_SPLUNK_IMAGE_CREATED"]; got != "2026-08-11T05:29:10Z" {
		t.Fatalf("generated UTC OCI creation time = %q, want %q", got, "2026-08-11T05:29:10Z")
	}
}

func TestGenerateEnvCreatesVerifiedClickHouseTLSIdentity(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skipf("openssl is unavailable: %v", err)
	}
	deployDirectory := deploymentDirectory(t)
	envFile := filepath.Join(t.TempDir(), "open-splunk.env")
	commandContext, cancelCommand := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelCommand()
	values := mustGenerateDeploymentEnvironment(
		t,
		commandContext,
		deployDirectory,
		envFile,
	)
	for _, name := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD",
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD",
	} {
		if password := values[name]; len(password) != 64 ||
			strings.Trim(password, "0123456789abcdef") != "" {
			t.Fatalf("generated %s is not 256-bit lowercase hex", name)
		}
	}
	for role, variable := range map[string]string{
		"backup":    "OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE",
		"migration": "OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE",
		"restore":   "OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE",
		"runtime":   "OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE",
		"deletion":  "OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE",
	} {
		path := values[variable]
		if !filepath.IsAbs(path) {
			t.Fatalf("generated %s password file path = %q, want absolute", role, path)
		}
		contents := readFile(t, path)
		if contents != values["OPEN_SPLUNK_CLICKHOUSE_"+strings.ToUpper(role)+"_PASSWORD"] {
			t.Fatalf("generated %s password file does not match its bootstrap credential", role)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat generated %s password file: %v", role, err)
		}
		if info.Mode().Perm() != 0o644 {
			t.Fatalf(
				"generated %s password permissions = %#o, want 0644 inside owner-only directory",
				role,
				info.Mode().Perm(),
			)
		}
	}
	if values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"] != "clickhouse" {
		t.Fatalf(
			"generated ClickHouse TLS server name = %q, want clickhouse",
			values["OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME"],
		)
	}
	if values["OPEN_SPLUNK_SERVER_TLS_SERVER_NAME"] != "open-splunk-server" {
		t.Fatalf(
			"generated server TLS name = %q, want open-splunk-server",
			values["OPEN_SPLUNK_SERVER_TLS_SERVER_NAME"],
		)
	}
	if version := values["OPEN_SPLUNK_APPLICATION_VERSION"]; version != "0.1.0" {
		t.Fatalf("generated application version = %q, want 0.1.0", version)
	}
	if revision := values["OPEN_SPLUNK_SOURCE_REVISION"]; len(revision) != 40 && len(revision) != 64 ||
		strings.Trim(revision, "0123456789abcdef") != "" {
		t.Fatalf("generated source revision is not a full lowercase Git object ID: %q", revision)
	}
	if _, err := time.Parse(time.RFC3339, values["OPEN_SPLUNK_IMAGE_CREATED"]); err != nil {
		t.Fatalf("generated OCI creation time is invalid: %v", err)
	}
	if epoch, err := strconv.ParseInt(values["OPEN_SPLUNK_SOURCE_DATE_EPOCH"], 10, 64); err != nil || epoch <= 0 {
		t.Fatalf("generated source epoch = %q, want positive Unix seconds", values["OPEN_SPLUNK_SOURCE_DATE_EPOCH"])
	}
	caFile := values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"]
	certificateFile := values["OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE"]
	privateKeyFile := values["OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE"]
	serverCAFile := values["OPEN_SPLUNK_SERVER_TLS_CA_FILE"]
	serverCertificateFile := values["OPEN_SPLUNK_SERVER_TLS_CERT_FILE"]
	serverPrivateKeyFile := values["OPEN_SPLUNK_SERVER_TLS_KEY_FILE"]
	administratorTokenFile := values["OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"]
	for name, path := range map[string]string{
		"ClickHouse CA":            caFile,
		"ClickHouse certificate":   certificateFile,
		"ClickHouse private key":   privateKeyFile,
		"server CA":                serverCAFile,
		"server certificate":       serverCertificateFile,
		"server private key":       serverPrivateKeyFile,
		"administrator token file": administratorTokenFile,
	} {
		if !filepath.IsAbs(path) {
			t.Fatalf("generated %s path = %q, want absolute", name, path)
		}
	}
	if serverCAFile != caFile {
		t.Fatalf("server CA file = %q, want shared deployment CA %q", serverCAFile, caFile)
	}
	pair, err := tls.LoadX509KeyPair(certificateFile, privateKeyFile)
	if err != nil {
		t.Fatalf("load generated ClickHouse TLS identity: %v", err)
	}
	if len(pair.Certificate) != 1 {
		t.Fatalf("generated server certificate chain length = %d, want 1", len(pair.Certificate))
	}
	certificate, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatalf("parse generated ClickHouse server certificate: %v", err)
	}
	for _, serverName := range []string{"clickhouse", "localhost", "127.0.0.1"} {
		if err := certificate.VerifyHostname(serverName); err != nil {
			t.Fatalf("verify generated ClickHouse server name %q: %v", serverName, err)
		}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(readFile(t, caFile))) {
		t.Fatal("generated ClickHouse CA file contains no certificates")
	}
	chains, err := certificate.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   "clickhouse",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		t.Fatalf("verify generated ClickHouse TLS chain: %v", err)
	}
	if len(chains) != 1 || len(chains[0]) != 2 {
		t.Fatalf("generated ClickHouse TLS chains = %#v, want one leaf+CA chain", chains)
	}
	serverPair, err := tls.LoadX509KeyPair(serverCertificateFile, serverPrivateKeyFile)
	if err != nil {
		t.Fatalf("load generated Open Splunk server TLS identity: %v", err)
	}
	if len(serverPair.Certificate) != 1 {
		t.Fatalf("generated Open Splunk server certificate chain length = %d, want 1", len(serverPair.Certificate))
	}
	serverCertificate, err := x509.ParseCertificate(serverPair.Certificate[0])
	if err != nil {
		t.Fatalf("parse generated Open Splunk server certificate: %v", err)
	}
	for _, serverName := range []string{"open-splunk-server", "localhost", "127.0.0.1"} {
		if err := serverCertificate.VerifyHostname(serverName); err != nil {
			t.Fatalf("verify generated Open Splunk server name %q: %v", serverName, err)
		}
	}
	serverChains, err := serverCertificate.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   "open-splunk-server",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		t.Fatalf("verify generated Open Splunk server TLS chain: %v", err)
	}
	if len(serverChains) != 1 || len(serverChains[0]) != 2 {
		t.Fatalf("generated Open Splunk server TLS chains = %#v, want one leaf+CA chain", serverChains)
	}
	administratorToken := strings.TrimSuffix(readFile(t, administratorTokenFile), "\n")
	if len(administratorToken) < 32 || len(administratorToken) > 512 ||
		strings.ContainsAny(administratorToken, " \t\r\n") {
		t.Fatal("generated administrator token does not satisfy the browser bearer-token boundary")
	}
	for name, password := range map[string]string{
		"bootstrap": values["OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD"],
		"migration": values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD"],
		"runtime":   values["OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD"],
		"deletion":  values["OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD"],
		"backup":    values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD"],
		"restore":   values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"],
	} {
		if administratorToken == password {
			t.Fatalf("administrator token duplicates the %s ClickHouse password", name)
		}
	}
	environmentInfo, err := os.Stat(envFile)
	if err != nil {
		t.Fatalf("stat generated environment: %v", err)
	}
	if environmentInfo.Mode().Perm() != 0o600 {
		t.Fatalf(
			"generated environment permissions = %#o, want 0600",
			environmentInfo.Mode().Perm(),
		)
	}
	for name, path := range map[string]string{
		"ClickHouse key":      privateKeyFile,
		"server key":          serverPrivateKeyFile,
		"administrator token": administratorTokenFile,
		"migration password":  values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE"],
		"runtime password":    values["OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE"],
		"deletion password":   values["OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE"],
		"backup password":     values["OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE"],
		"restore password":    values["OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE"],
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat generated %s: %v", name, err)
		}
		if info.Mode().Perm()&0o022 != 0 {
			t.Fatalf("generated %s permissions = %#o, want no group/other writes", name, info.Mode().Perm())
		}
	}
	tlsDirectoryInfo, err := os.Stat(filepath.Dir(privateKeyFile))
	if err != nil {
		t.Fatalf("stat generated TLS directory: %v", err)
	}
	if tlsDirectoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf(
			"generated TLS directory permissions = %#o, want 0700",
			tlsDirectoryInfo.Mode().Perm(),
		)
	}
	tlsEntries, err := os.ReadDir(filepath.Dir(privateKeyFile))
	if err != nil {
		t.Fatalf("read generated TLS directory: %v", err)
	}
	generatedTLSFiles := make([]string, 0, len(tlsEntries))
	for _, entry := range tlsEntries {
		generatedTLSFiles = append(generatedTLSFiles, entry.Name())
	}
	sort.Strings(generatedTLSFiles)
	wantedTLSFiles := []string{
		"administrator.token",
		"ca.crt",
		"clickhouse-backup.password",
		"clickhouse-deletion.password",
		"clickhouse-migration.password",
		"clickhouse-restore.password",
		"clickhouse-runtime.password",
		"open-splunk-server.crt",
		"open-splunk-server.key",
		"server.crt",
		"server.key",
	}
	if len(generatedTLSFiles) != len(wantedTLSFiles) {
		t.Fatalf(
			"generated TLS files = %v, want %v",
			generatedTLSFiles,
			wantedTLSFiles,
		)
	}
	for index := range wantedTLSFiles {
		if generatedTLSFiles[index] != wantedTLSFiles[index] {
			t.Fatalf(
				"generated TLS files = %v, want %v",
				generatedTLSFiles,
				wantedTLSFiles,
			)
		}
	}

	secondContext, cancelSecond := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelSecond()
	second := newGenerateEnvCommand(secondContext, deployDirectory, envFile)
	if secondOutput, secondErr := second.CombinedOutput(); secondErr == nil ||
		!strings.Contains(string(secondOutput), "refusing to overwrite") {
		t.Fatalf(
			"second environment generation = (%v, %q), want overwrite rejection",
			secondErr,
			secondOutput,
		)
	}
}

func TestGenerateEnvQuotesSafePathsAndRejectsShellMetacharacters(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skipf("openssl is unavailable: %v", err)
	}
	deployDirectory := deploymentDirectory(t)
	spaceDirectory := filepath.Join(t.TempDir(), "directory with spaces")
	if err := os.Mkdir(spaceDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	envFile := filepath.Join(spaceDirectory, "open splunk.env")
	commandContext, cancelCommand := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelCommand()
	values := mustGenerateDeploymentEnvironment(
		t,
		commandContext,
		deployDirectory,
		envFile,
	)
	wantedCAFile := envFile + ".tls/ca.crt"
	if values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"] != wantedCAFile {
		t.Fatalf(
			"generated CA path = %q, want %q",
			values["OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE"],
			wantedCAFile,
		)
	}
	sourceContext, cancelSource := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelSource()
	source := exec.CommandContext(
		sourceContext,
		"sh",
		"-c",
		`. "$1" && test "$OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE" = "$2"`,
		"open-splunk-env-check",
		envFile,
		wantedCAFile,
	)
	if output, err := source.CombinedOutput(); err != nil {
		t.Fatalf("source generated environment safely: %v: %s", err, output)
	}

	unsafeDirectory := filepath.Join(t.TempDir(), "unsafe;directory")
	if err := os.Mkdir(unsafeDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	unsafeEnvFile := filepath.Join(unsafeDirectory, "open-splunk.env")
	unsafeContext, cancelUnsafe := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelUnsafe()
	unsafe := newGenerateEnvCommand(unsafeContext, deployDirectory, unsafeEnvFile)
	unsafeOutput, unsafeErr := unsafe.CombinedOutput()
	if unsafeErr == nil ||
		!strings.Contains(string(unsafeOutput), "shell-unsafe characters") {
		t.Fatalf(
			"unsafe output path generation = (%v, %q), want rejection",
			unsafeErr,
			unsafeOutput,
		)
	}
	for _, path := range []string{unsafeEnvFile, unsafeEnvFile + ".tls"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unsafe generation path %q exists: %v", path, err)
		}
	}
}

func TestGenerateEnvConcurrentInvocationsCannotOverwrite(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skipf("openssl is unavailable: %v", err)
	}
	deployDirectory := deploymentDirectory(t)
	envFile := filepath.Join(t.TempDir(), "open-splunk.env")
	commandContext, cancelCommands := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelCommands()
	type commandResult struct {
		output []byte
		err    error
	}
	results := make(chan commandResult, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			command := newGenerateEnvCommand(
				commandContext,
				deployDirectory,
				envFile,
			)
			output, commandErr := command.CombinedOutput()
			results <- commandResult{output: output, err: commandErr}
		}()
	}
	close(start)
	successes := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			successes++
			continue
		}
		if !strings.Contains(string(result.output), "refusing to overwrite") {
			t.Fatalf(
				"concurrent generator failure = %v: %s",
				result.err,
				result.output,
			)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent generator successes = %d, want 1", successes)
	}
	values := parseDeploymentEnvironment(t, readFile(t, envFile))
	for _, name := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE",
		"OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE",
	} {
		if _, err := os.Stat(values[name]); err != nil {
			t.Fatalf("stat concurrently generated %s: %v", name, err)
		}
	}
}

func TestGenerateEnvClearsDarwinInheritedACLs(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin extended ACL inheritance is platform-specific")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skipf("openssl is unavailable: %v", err)
	}

	parent := t.TempDir()
	aclCommand := exec.CommandContext(
		t.Context(),
		"/bin/chmod",
		"+a",
		"everyone allow read,file_inherit,directory_inherit",
		parent,
	)
	if output, err := aclCommand.CombinedOutput(); err != nil {
		t.Fatalf("add inherited Darwin ACL: %v: %s", err, output)
	}
	if !hasDarwinExtendedACL(t, parent) {
		t.Fatal("Darwin ACL fixture has no extended ACL")
	}

	envFile := filepath.Join(parent, "open-splunk.env")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	values := mustGenerateDeploymentEnvironment(
		t,
		ctx,
		deploymentDirectory(t),
		envFile,
	)
	for name, path := range map[string]string{
		"environment":         envFile,
		"TLS directory":       envFile + ".tls",
		"administrator token": values["OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"],
		"ClickHouse key":      values["OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE"],
		"migration password":  values["OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE"],
	} {
		if hasDarwinExtendedACL(t, path) {
			t.Errorf("generated %s retains an inherited extended ACL", name)
		}
	}
}

func hasDarwinExtendedACL(t *testing.T, path string) bool {
	t.Helper()
	command := exec.CommandContext(t.Context(), "/bin/ls", "-lde", path)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect Darwin ACL on %q: %v: %s", path, err, output)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		t.Fatalf("inspect Darwin ACL on %q returned no mode", path)
	}
	return len(lines) > 1
}

func TestDeploymentClickHouseTLSListenerContract(t *testing.T) {
	t.Parallel()

	config := readFile(
		t,
		filepath.Join(
			"..",
			"..",
			"deploy",
			"clickhouse-config",
			"tls.xml",
		),
	)
	for _, fragment := range []string{
		"<tcp_port_secure>9440</tcp_port_secure>",
		"<certificateFile>/etc/clickhouse-server/tls/server.crt</certificateFile>",
		"<privateKeyFile>/etc/clickhouse-server/tls/server.key</privateKeyFile>",
		"<verificationMode>none</verificationMode>",
		"<loadDefaultCAFile>false</loadDefaultCAFile>",
		"<disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>",
		"<name>RejectCertificateHandler</name>",
	} {
		if !strings.Contains(config, fragment) {
			t.Errorf("ClickHouse TLS config is missing contract fragment %q", fragment)
		}
	}
	for _, prohibited := range []string{
		"AcceptCertificateHandler",
		"<verificationMode>relaxed</verificationMode>",
		"<verificationMode>once</verificationMode>",
	} {
		if strings.Contains(config, prohibited) {
			t.Errorf("ClickHouse TLS config contains prohibited fragment %q", prohibited)
		}
	}

	clientConfig := readFile(
		t,
		filepath.Join(
			"..",
			"..",
			"deploy",
			"clickhouse-config",
			"client-tls.xml",
		),
	)
	for _, fragment := range []string{
		"<caConfig>/etc/clickhouse-client/open-splunk-ca.crt</caConfig>",
		"<loadDefaultCAFile>false</loadDefaultCAFile>",
		"<verificationMode>strict</verificationMode>",
		"<disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>",
		"<name>RejectCertificateHandler</name>",
	} {
		if !strings.Contains(clientConfig, fragment) {
			t.Errorf("ClickHouse TLS client config is missing contract fragment %q", fragment)
		}
	}
	if strings.Contains(clientConfig, "AcceptCertificateHandler") ||
		strings.Contains(clientConfig, "<verificationMode>none</verificationMode>") {
		t.Error("ClickHouse TLS client config weakens certificate verification")
	}
}

func parseDeploymentEnvironment(t *testing.T, contents string) map[string]string {
	t.Helper()
	values := make(map[string]string)
	for _, line := range strings.Split(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) != name || name == "" || value == "" {
			t.Fatalf("generated environment line is invalid: %q", line)
		}
		if strings.HasPrefix(value, "\"") {
			decoded, err := strconv.Unquote(value)
			if err != nil {
				t.Fatalf("generated environment value is invalid: %q: %v", line, err)
			}
			value = decoded
		}
		if _, exists := values[name]; exists {
			t.Fatalf("generated environment repeats %q", name)
		}
		values[name] = value
	}
	return values
}

func TestDeploymentClickHouseBootstrapSeparatesServicePrincipals(t *testing.T) {
	t.Parallel()

	bootstrap := readFile(
		t,
		filepath.Join("..", "..", "deploy", "clickhouse-init.sh"),
	)
	for _, fragment := range []string{
		"CREATE USER IF NOT EXISTS open_splunk_migrator",
		"GRANT CREATE DATABASE ON open_splunk.* TO open_splunk_migrator",
		"GRANT SHOW TABLES ON open_splunk.* TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.recovery_sets TO open_splunk_migrator",
		"GRANT CREATE TABLE ON open_splunk.recovery_archive_markers TO open_splunk_migrator",
		"GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX ON open_splunk.events TO open_splunk_migrator",
		"CREATE USER IF NOT EXISTS open_splunk_runtime",
		"GRANT SELECT, INSERT ON open_splunk.events TO open_splunk_runtime",
		"GRANT SELECT(database, table, active, rows, bytes_on_disk) ON system.parts TO open_splunk_runtime",
		"CREATE USER IF NOT EXISTS open_splunk_deletion",
		"GRANT ALTER DELETE, SELECT(tenant_id, index_name) ON open_splunk.events TO open_splunk_deletion",
		"GRANT SELECT ON system.tables TO open_splunk_deletion",
		"GRANT SELECT ON system.mutations TO open_splunk_deletion",
		"expected_server_version=26.7.3.19",
		"SELECT version()",
		"chmod 0700 /var/lib/clickhouse /var/log/clickhouse-server",
	} {
		if !strings.Contains(bootstrap, fragment) {
			t.Errorf(
				"ClickHouse bootstrap is missing principal contract fragment %q",
				fragment,
			)
		}
	}
	for _, prohibited := range []string{
		"--password",
		"/open-splunk-migrations",
		"for migration",
		"CREATE DATABASE IF NOT EXISTS",
		"CREATE TABLE IF NOT EXISTS",
		"ALTER TABLE open_splunk",
		"INSERT INTO open_splunk.schema_migrations",
		"GRANT ALL",
		"GRANT ALTER ON open_splunk.events",
		"GRANT SELECT ON system.parts TO open_splunk_runtime",
		"GRANT SELECT ON system.* TO open_splunk_runtime",
		"GRANT DROP",
		"GRANT TRUNCATE",
		"WITH GRANT OPTION",
		"WITH ADMIN OPTION",
		"CREATE ROLE ",
		"DEFAULT ROLE",
	} {
		if strings.Contains(bootstrap, prohibited) {
			t.Errorf(
				"ClickHouse bootstrap contains prohibited broad grant %q",
				prohibited,
			)
		}
	}

	accessConfig := readFile(
		t,
		filepath.Join(
			"..",
			"..",
			"deploy",
			"clickhouse-config",
			"access.xml",
		),
	)
	if !strings.Contains(
		accessConfig,
		"<select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>",
	) {
		t.Error("ClickHouse deployment does not require explicit system-table grants")
	}

	bootstrapUser := readFile(
		t,
		filepath.Join(
			"..",
			"..",
			"deploy",
			"clickhouse-config",
			"bootstrap-user.xml",
		),
	)
	for _, fragment := range []string{
		`<default remove="remove"/>`,
		`<password from_env="CLICKHOUSE_PASSWORD"/>`,
		"<ip>127.0.0.1</ip>",
		"<ip>::1</ip>",
		"<access_management>1</access_management>",
	} {
		if !strings.Contains(bootstrapUser, fragment) {
			t.Errorf(
				"ClickHouse bootstrap user is missing safety fragment %q",
				fragment,
			)
		}
	}
	if strings.Contains(bootstrapUser, "::/0") ||
		strings.Contains(bootstrapUser, "0.0.0.0/0") {
		t.Error("ClickHouse bootstrap administrator must remain loopback-only")
	}
}

func TestDeploymentClickHouseBootstrapRejectsReusedCredentials(t *testing.T) {
	t.Parallel()

	sharedPassword := strings.Repeat("a", 64)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		"sh",
		filepath.Join("..", "..", "deploy", "clickhouse-init.sh"),
	)
	command.WaitDelay = time.Second
	command.Env = append(
		os.Environ(),
		"CLICKHOUSE_USER=open_splunk_bootstrap",
		"CLICKHOUSE_PASSWORD="+sharedPassword,
		"OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD="+sharedPassword,
		"OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD="+strings.Repeat("b", 64),
		"OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD="+strings.Repeat("c", 64),
		"OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD="+strings.Repeat("d", 64),
		"OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD="+strings.Repeat("e", 64),
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("clickhouse-init.sh accepted a reused bootstrap credential")
	}
	if !strings.Contains(
		string(output),
		"bootstrap, migration, runtime, deletion, backup, and restore passwords must be distinct",
	) {
		t.Fatalf("clickhouse-init.sh output = %q", output)
	}
}

// TestMigrationsAgainstClickHouse is intentionally opt-in because it starts a
// real ClickHouse container and may pull a large image. Run it with:
//
//	OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test ./migrations/clickhouse -run AgainstClickHouse -v
func TestMigrationsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := randomHex(t, 6)
	container := "open-splunk-clickhouse-migrations-" + suffix
	password := randomHex(t, 24)
	t.Setenv("CLICKHOUSE_PASSWORD", password)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = testsupport.DefaultClickHouseImage
	}

	runDocker(ctx, t, nil,
		"run", "--detach", "--rm",
		"--name", container,
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD",
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		"--env", "CLICKHOUSE_LOG_TO_CONSOLE=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		output, err := exec.CommandContext(
			cleanupCtx,
			"docker",
			"rm",
			"--force",
			"--volumes",
			container,
		).CombinedOutput()
		if err != nil && !bytes.Contains(bytes.ToLower(output), []byte("no such container")) {
			t.Errorf("remove test container: %v: %s", err, output)
		}
	})

	waitForClickHouse(ctx, t, container, password)
	files := migrationFiles(t)
	runClickHouse(ctx, t, container, password, readFile(t, files[0]))
	legacyInsert := `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time, expires_at
		)
		VALUES
		(
			'legacy-event', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY
		)`
	runClickHouse(ctx, t, container, password, legacyInsert)
	for _, path := range files[1:] {
		runClickHouse(ctx, t, container, password, readFile(t, path))
	}
	// Every migration must remain idempotent after the populated upgrade path.
	for _, path := range files {
		runClickHouse(ctx, t, container, password, readFile(t, path))
	}

	columns := clickHouseQuery(ctx, t, container, password, `
		SELECT name
		FROM system.columns
		WHERE database = 'open_splunk' AND table = 'events'
		ORDER BY position
		FORMAT TSVRaw`)
	for _, name := range []string{
		"event_id", "tenant_id", "index_name", "event_time", "index_time", "raw",
		"fields", "field_names", "field_types", "field_metadata_version", "collector_id", "batch_id",
		"visibility_seq", "expires_at",
	} {
		if !strings.Contains(columns, name) {
			t.Errorf("live events schema is missing column %q; columns: %s", name, columns)
		}
	}

	insert := `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			host, source, sourcetype, severity, raw, raw_encoding,
			fields, field_names, field_types, field_metadata_version,
			collector_id, batch_id, batch_sequence, expires_at, visibility_seq
		)
		SETTINGS
			insert_deduplication_token = 'migration-smoke-batch',
			input_format_json_read_numbers_as_strings = 0,
			input_format_json_read_bools_as_numbers = 0,
			input_format_json_read_bools_as_strings = 0,
			input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1,
			input_format_try_infer_dates = 0,
			input_format_try_infer_datetimes = 0
		FORMAT JSONEachRow
		{"event_id":"event-1","tenant_id":"tenant-1","index_name":"main","event_time":"2026-07-21 03:04:05.123456789","index_time":"2026-07-21 03:04:06.123","host":"host-1","source":"app.log","sourcetype":"go:zap:json","severity":3,"raw":"{\"message\":\"hello\"}","raw_encoding":1,"fields":{"big":9007199254740993,"unsigned":18446744073709551615,"negative":-9223372036854775808,"ratio":1.25,"ok":true,"nothing":null,"mixed":[1,"two",true],"nested":{"label":"kept"}},"field_names":["big","mixed","negative","nested.label","nothing","ok","ratio","unsigned"],"field_types":[3,10,3,2,1,6,5,4],"field_metadata_version":1,"collector_id":"collector-1","batch_id":"batch-1","batch_sequence":1,"expires_at":"2099-02-01 03:04:06.123","visibility_seq":1}`

	// A retry must contain the exact same accepted rows in the same order and
	// reuse the exact token. The non-replicated MergeTree window then drops it.
	runClickHouse(ctx, t, container, password, insert)
	runClickHouse(ctx, t, container, password, insert)

	legacyVisibility := clickHouseQuery(ctx, t, container, password, `
		SELECT visibility_seq
		FROM open_splunk.events
		WHERE event_id = 'legacy-event'
		FORMAT TSVRaw`)
	if legacyVisibility != "0" {
		t.Fatalf("pre-migration row visibility = %q, want migration default 0", legacyVisibility)
	}
	legacyMetadata := clickHouseQuery(ctx, t, container, password, `
		SELECT concat(toString(field_metadata_version), ':', toJSONString(field_types))
		FROM open_splunk.events
		WHERE event_id = 'legacy-event'
		FORMAT TSVRaw`)
	if legacyMetadata != "0:[]" {
		t.Fatalf("pre-migration row field metadata = %q, want version-zero empty types", legacyMetadata)
	}
	legacyError := clickHouseQueryError(ctx, t, container, password, strings.Replace(legacyInsert, "legacy-event", "post-upgrade-legacy-event", 1))
	if !strings.Contains(legacyError, "visibility_seq_is_positive") {
		t.Fatalf("legacy writer omission did not fail the visibility constraint: %s", legacyError)
	}
	invalidMetadataPrefix := `
		INSERT INTO open_splunk.events
		(event_id, tenant_id, index_name, event_time, index_time, expires_at, visibility_seq,
		 field_names, field_types, field_metadata_version)
		VALUES `
	for name, values := range map[string]string{
		"unsupported-version": "('unsupported-version', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY, 1, ['x'], [2], 2)",
		"misaligned":          "('misaligned', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY, 1, ['x'], [], 1)",
		"invalid-type":        "('invalid-type', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY, 1, ['x'], [13], 1)",
		"typed-legacy":        "('typed-legacy', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY, 1, ['x'], [2], 0)",
	} {
		errorText := clickHouseQueryError(ctx, t, container, password, invalidMetadataPrefix+values)
		if !strings.Contains(errorText, "field_metadata_") {
			t.Errorf("invalid field metadata case %q was not rejected by metadata constraint: %s", name, errorText)
		}
	}

	rowCount := clickHouseQuery(ctx, t, container, password, `
		SELECT count()
		FROM open_splunk.events
		WHERE event_id = 'event-1'
		FORMAT TSVRaw`)
	if rowCount != "1" {
		t.Fatalf("batch retry produced %q stored rows; want 1", rowCount)
	}

	got := clickHouseQuery(ctx, t, container, password, `
		SELECT
			dynamicType(fields.big),
			fields.big.:Int64,
			dynamicType(fields.unsigned),
			fields.unsigned.:UInt64,
			dynamicType(fields.negative),
			fields.negative.:Int64,
			dynamicType(fields.ratio),
			fields.ratio.:Float64,
			dynamicType(fields.ok),
			fields.ok.:Bool,
			dynamicType(fields.nothing),
			dynamicType(fields.mixed),
			fields.nested.label.:String
		FROM open_splunk.events
		WHERE event_id = 'event-1'
		LIMIT 1
		FORMAT TSVRaw`)
	if want := "Int64\t9007199254740993\tUInt64\t18446744073709551615\tInt64\t-9223372036854775808\tFloat64\t1.25\tBool\ttrue\tNone\tArray(Dynamic)\tkept"; got != want {
		t.Fatalf("typed JSON contract mismatch\n got: %q\nwant: %q", got, want)
	}
	metadata := clickHouseQuery(ctx, t, container, password, `
		SELECT concat(toString(field_metadata_version), ':', toJSONString(field_types))
		FROM open_splunk.events
		WHERE event_id = 'event-1'
		FORMAT TSVRaw`)
	if metadata != "1:[3,10,3,2,1,6,5,4]" {
		t.Fatalf("field metadata contract = %q", metadata)
	}

	versions := clickHouseQuery(ctx, t, container, password, `
		SELECT groupArray((version, count))
		FROM
		(
			SELECT version, count() AS count
			FROM open_splunk.schema_migrations
			GROUP BY version
			ORDER BY version
		)
		FORMAT TSVRaw`)
	var expectedVersions strings.Builder
	expectedVersions.WriteByte('[')
	for index := range migrationFiles(t) {
		if index > 0 {
			expectedVersions.WriteByte(',')
		}
		fmt.Fprintf(&expectedVersions, "(%d,1)", index+1)
	}
	expectedVersions.WriteByte(']')
	if versions != expectedVersions.String() {
		t.Fatalf("migration ledger = %q, want one row for each version", versions)
	}
}

func migrationFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("[0-9][0-9][0-9][0-9]_*.sql")
	if err != nil {
		t.Fatalf("glob ClickHouse migrations: %v", err)
	}
	sort.Strings(files)
	return files
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
}

func randomHex(t *testing.T, byteCount int) string {
	t.Helper()
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate random test identifier: %v", err)
	}
	return hex.EncodeToString(value)
}

func waitForClickHouse(ctx context.Context, t *testing.T, container, password string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	consecutiveSuccesses := 0
	for time.Now().Before(deadline) {
		cmd := exec.CommandContext(ctx, "docker", "exec", container,
			"clickhouse-client", "--host", "127.0.0.1",
			"--user", "open_splunk", "--password", password,
			"--query", "SELECT 1",
		)
		if output, err := cmd.CombinedOutput(); err == nil && strings.TrimSpace(string(output)) == "1" {
			consecutiveSuccesses++
			// The image entrypoint briefly exposes an initialization server before
			// replacing it with the long-running server. Require a stable window so
			// migrations never race that intentional restart.
			if consecutiveSuccesses >= 5 {
				return
			}
		} else {
			consecutiveSuccesses = 0
			if err != nil {
				lastErr = fmt.Errorf("%w: %s", err, bytes.TrimSpace(output))
			} else {
				lastErr = fmt.Errorf("unexpected response: %s", bytes.TrimSpace(output))
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for ClickHouse: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("ClickHouse did not become ready: %v", lastErr)
}

func runClickHouse(ctx context.Context, t *testing.T, container, password, sql string) {
	t.Helper()
	runDocker(ctx, t, strings.NewReader(sql),
		"exec", "--interactive", container,
		"clickhouse-client", "--host", "127.0.0.1",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
}

func clickHouseQuery(ctx context.Context, t *testing.T, container, password, query string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", "exec", container,
		"clickhouse-client", "--host", "127.0.0.1",
		"--user", "open_splunk", "--password", password,
		"--query", query,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run ClickHouse query: %v\n%s\ncontainer logs:\n%s", err, output, dockerLogs(container))
	}
	return strings.TrimSpace(string(output))
}

func clickHouseQueryError(ctx context.Context, t *testing.T, container, password, query string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", "exec", container,
		"clickhouse-client", "--host", "127.0.0.1",
		"--user", "open_splunk", "--password", password,
		"--query", query,
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("ClickHouse query unexpectedly succeeded: %s", query)
	}
	return string(output)
}

func runDocker(ctx context.Context, t *testing.T, stdin io.Reader, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		logs := ""
		if len(args) >= 3 && args[0] == "exec" {
			logs = "\ncontainer logs:\n" + dockerLogs(args[2])
		}
		t.Fatalf("docker %s: %v\n%s%s", strings.Join(args, " "), err, output, logs)
	}
}

func dockerLogs(container string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, stateErr := exec.CommandContext(ctx, "docker", "inspect", "--format",
		"status={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}} oom={{.State.OOMKilled}}", container,
	).CombinedOutput()
	output, err := exec.CommandContext(ctx, "docker", "logs", "--tail", "200", container).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("state=%s (error %v); logs unavailable: %v: %s", state, stateErr, err, output)
	}
	return fmt.Sprintf("%s%s", state, output)
}
