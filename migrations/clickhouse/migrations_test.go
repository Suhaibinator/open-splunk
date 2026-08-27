package clickhouse_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

var migrationNamePattern = regexp.MustCompile(`^(\d{4})_[a-z0-9]+(?:_[a-z0-9]+)*\.sql$`)

func TestMigrationFilesAreContiguous(t *testing.T) {
	files := migrationFiles(t)
	if len(files) == 0 {
		t.Fatal("no ClickHouse migrations found")
	}
	for index, path := range files {
		match := migrationNamePattern.FindStringSubmatch(filepath.Base(path))
		if match == nil {
			t.Fatalf("migration %q must match %s", filepath.Base(path), migrationNamePattern)
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("parse migration version in %q: %v", path, err)
		}
		if want := index + 1; version != want {
			t.Fatalf("migration %q has version %d; want %d", path, version, want)
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

func TestBaselineSchemaContract(t *testing.T) {
	sql := readFile(t, "0001_baseline.sql")
	for _, fragment := range []string{
		"CREATE DATABASE IF NOT EXISTS open_splunk",
		"CREATE TABLE IF NOT EXISTS open_splunk.schema_migrations",
		"CREATE TABLE IF NOT EXISTS open_splunk.events",
		"`fields` JSON(",
		"PARTITION BY toYYYYMM(`event_time`)",
		"TTL `expires_at` DELETE",
		"INDEX idx_raw_text",
		"INDEX idx_visibility_seq",
		"CREATE TABLE IF NOT EXISTS open_splunk.recovery_sets",
		"CREATE TABLE IF NOT EXISTS open_splunk.recovery_archive_markers",
		"SELECT 1, 'baseline', now64(3)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("baseline is missing schema contract fragment %q", fragment)
		}
	}
}

func TestSimpleExternalClickHouseComposeContract(t *testing.T) {
	compose := readFile(t, filepath.Join("..", "..", "deploy", "docker-compose.yaml"))
	if services := composeServiceNames(compose); !slices.Equal(services, []string{"server"}) {
		t.Fatalf("default Compose services = %v, want only server", services)
	}
	for _, fragment := range []string{
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN:",
		"OPEN_SPLUNK_CLICKHOUSE_PASSWORD:",
		"${OPEN_SPLUNK_CLICKHOUSE_ADDRESS:-per-clickhouse:9000}",
		"${OPEN_SPLUNK_CLICKHOUSE_USERNAME:-clickhouse}",
		"-clickhouse-username",
		"http://127.0.0.1:8080/readyz",
		"per-obs-network",
	} {
		if !strings.Contains(compose, fragment) {
			t.Errorf("simple Compose file is missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"clickhouse-migrator:",
		"server-bootstrap:",
		"deployment-backup:",
		"deployment-restore:",
		"-clickhouse-secure",
		"-clickhouse-skip-migrations",
		".password:ro",
		".crt:ro",
		".key:ro",
	} {
		if strings.Contains(compose, forbidden) {
			t.Errorf("simple Compose file retained obsolete fragment %q", forbidden)
		}
	}
}

func composeServiceNames(compose string) []string {
	lines := strings.Split(compose, "\n")
	inServices := false
	var services []string
	for _, line := range lines {
		if line == "services:" {
			inServices = true
			continue
		}
		if !inServices {
			continue
		}
		if line != "" && line[0] != ' ' {
			break
		}
		if strings.HasPrefix(line, "  ") &&
			!strings.HasPrefix(line, "    ") &&
			strings.HasSuffix(line, ":") {
			services = append(services, strings.TrimSuffix(strings.TrimSpace(line), ":"))
		}
	}
	return services
}

func TestGenerateDevelopmentEnvironmentUsesSinglePlaintextCredential(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), ".env.development")
	generator := filepath.Join("..", "..", "deploy", "generate-env.sh")
	command := exec.Command(generator, "--development", outputPath)
	command.Env = append(
		os.Environ(),
		"OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT=19000",
		"OPEN_SPLUNK_SERVER_HTTP_PORT=18080",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generate development environment: %v: %s", err, output)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("development environment mode = %#o, want 0600", info.Mode().Perm())
	}
	contents := readFile(t, outputPath)
	for _, name := range []string{
		"OPEN_SPLUNK_CLICKHOUSE_ADDRESS=127.0.0.1:19000",
		"OPEN_SPLUNK_CLICKHOUSE_USERNAME=clickhouse",
		"OPEN_SPLUNK_CLICKHOUSE_PASSWORD=",
		"OPEN_SPLUNK_ADMINISTRATOR_TOKEN=",
		"OPEN_SPLUNK_SERVER_HTTP_PORT=18080",
	} {
		if !strings.Contains(contents, name) {
			t.Errorf("development environment is missing %q", name)
		}
	}
	for _, forbidden := range []string{"TLS", "MIGRATION_PASSWORD", "RUNTIME_PASSWORD", "DELETION_PASSWORD"} {
		if strings.Contains(contents, forbidden) {
			t.Errorf("development environment retained obsolete setting %q", forbidden)
		}
	}
}

// TestMigrationsAgainstClickHouse is opt-in because it starts the pinned
// ClickHouse image. It proves one plaintext account can apply the embedded
// migrations idempotently and then use the application database.
func TestMigrationsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker integration was requested but the CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	container, err := testsupport.StartClickHouseWithServicePrincipals(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close ClickHouse fixture: %v", closeErr)
		}
	})

	options := &clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: "default",
			Username: container.Username,
			Password: container.Password,
		},
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if err := server.ApplyClickHouseMigrations(ctx, connection, migrations.ClickHouse()); err != nil {
		t.Fatal(err)
	}
	if err := server.ApplyClickHouseMigrations(ctx, connection, migrations.ClickHouse()); err != nil {
		t.Fatalf("repeat migrations: %v", err)
	}
	var count uint64
	if err := connection.QueryRow(ctx,
		"SELECT count() FROM open_splunk.schema_migrations",
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != uint64(len(migrationFiles(t))) {
		t.Fatalf("migration ledger rows = %d, want %d", count, len(migrationFiles(t)))
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
