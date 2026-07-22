package clickhouse_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const pinnedClickHouseImage = "clickhouse/clickhouse-server:26.3.17.4"

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
		"TYPE text(tokenizer = 'splitByNonAlpha')",
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

func TestComposeIsPinnedAndLoopbackOnly(t *testing.T) {
	compose := readFile(t, filepath.Join("..", "..", "deploy", "docker-compose.yaml"))

	for _, fragment := range []string{
		"image: " + pinnedClickHouseImage,
		"127.0.0.1:${OPEN_SPLUNK_CLICKHOUSE_HTTP_PORT:-8123}:8123",
		"127.0.0.1:${OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT:-9000}:9000",
		"OPEN_SPLUNK_CLICKHOUSE_PASSWORD:?",
		"healthcheck:",
	} {
		if !strings.Contains(compose, fragment) {
			t.Errorf("deployment compose file is missing safety contract fragment %q", fragment)
		}
	}
	if strings.Contains(compose, ":latest") || strings.Contains(compose, "0.0.0.0:") {
		t.Error("deployment compose file must not use a floating image or a wildcard host bind")
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
		image = pinnedClickHouseImage
	}

	runDocker(t, ctx, nil,
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
		output, err := exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).CombinedOutput()
		if err != nil && !bytes.Contains(bytes.ToLower(output), []byte("no such container")) {
			t.Errorf("remove test container: %v: %s", err, output)
		}
	})

	waitForClickHouse(t, ctx, container, password)
	files := migrationFiles(t)
	runClickHouse(t, ctx, container, password, readFile(t, files[0]))
	legacyInsert := `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time, expires_at
		)
		VALUES
		(
			'legacy-event', 'tenant-1', 'main', now64(9), now64(3), now64(3) + INTERVAL 1 DAY
		)`
	runClickHouse(t, ctx, container, password, legacyInsert)
	for _, path := range files[1:] {
		runClickHouse(t, ctx, container, password, readFile(t, path))
	}
	// Every migration must remain idempotent after the populated upgrade path.
	for _, path := range files {
		runClickHouse(t, ctx, container, password, readFile(t, path))
	}

	columns := clickHouseQuery(t, ctx, container, password, `
		SELECT name
		FROM system.columns
		WHERE database = 'open_splunk' AND table = 'events'
		ORDER BY position
		FORMAT TSVRaw`)
	for _, name := range []string{
		"event_id", "tenant_id", "index_name", "event_time", "index_time", "raw",
		"fields", "field_names", "collector_id", "batch_id", "visibility_seq", "expires_at",
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
			fields, field_names, collector_id, batch_id, batch_sequence, expires_at, visibility_seq
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
		{"event_id":"event-1","tenant_id":"tenant-1","index_name":"main","event_time":"2026-07-21 03:04:05.123456789","index_time":"2026-07-21 03:04:06.123","host":"host-1","source":"app.log","sourcetype":"go:zap:json","severity":3,"raw":"{\"message\":\"hello\"}","raw_encoding":1,"fields":{"big":9007199254740993,"unsigned":18446744073709551615,"negative":-9223372036854775808,"ratio":1.25,"ok":true,"nothing":null,"mixed":[1,"two",true],"nested":{"label":"kept"}},"field_names":["big","mixed","negative","nested.label","nothing","ok","ratio","unsigned"],"collector_id":"collector-1","batch_id":"batch-1","batch_sequence":1,"expires_at":"2099-02-01 03:04:06.123","visibility_seq":1}`

	// A retry must contain the exact same accepted rows in the same order and
	// reuse the exact token. The non-replicated MergeTree window then drops it.
	runClickHouse(t, ctx, container, password, insert)
	runClickHouse(t, ctx, container, password, insert)

	legacyVisibility := clickHouseQuery(t, ctx, container, password, `
		SELECT visibility_seq
		FROM open_splunk.events
		WHERE event_id = 'legacy-event'
		FORMAT TSVRaw`)
	if legacyVisibility != "0" {
		t.Fatalf("pre-migration row visibility = %q, want migration default 0", legacyVisibility)
	}
	legacyError := clickHouseQueryError(t, ctx, container, password, strings.Replace(legacyInsert, "legacy-event", "post-upgrade-legacy-event", 1))
	if !strings.Contains(legacyError, "visibility_seq_is_positive") {
		t.Fatalf("legacy writer omission did not fail the visibility constraint: %s", legacyError)
	}

	rowCount := clickHouseQuery(t, ctx, container, password, `
		SELECT count()
		FROM open_splunk.events
		WHERE event_id = 'event-1'
		FORMAT TSVRaw`)
	if rowCount != "1" {
		t.Fatalf("batch retry produced %q stored rows; want 1", rowCount)
	}

	got := clickHouseQuery(t, ctx, container, password, `
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

	versions := clickHouseQuery(t, ctx, container, password, `
		SELECT groupArray((version, count))
		FROM
		(
			SELECT version, count() AS count
			FROM open_splunk.schema_migrations
			GROUP BY version
			ORDER BY version
		)
		FORMAT TSVRaw`)
	if versions != "[(1,1),(2,1)]" {
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

func waitForClickHouse(t *testing.T, ctx context.Context, container, password string) {
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

func runClickHouse(t *testing.T, ctx context.Context, container, password, sql string) {
	t.Helper()
	runDocker(t, ctx, strings.NewReader(sql),
		"exec", "--interactive", container,
		"clickhouse-client", "--host", "127.0.0.1",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
}

func clickHouseQuery(t *testing.T, ctx context.Context, container, password, query string) string {
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

func clickHouseQueryError(t *testing.T, ctx context.Context, container, password, query string) string {
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

func runDocker(t *testing.T, ctx context.Context, stdin io.Reader, args ...string) {
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
