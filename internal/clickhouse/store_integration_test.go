package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

const storeIntegrationImage = "clickhouse/clickhouse-server:26.3.17.4"

// TestStoreAgainstClickHouse is opt-in because it starts an ephemeral Docker
// container and may pull the pinned ClickHouse image.
func TestStoreAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container := "open-splunk-store-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPath := filepath.Join("..", "..", "migrations", "clickhouse", "0001_create_events.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	integrationDocker(t, ctx, bytes.NewReader(migration),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
	address := integrationNativeAddress(t, ctx, container)
	config := DefaultConfig()
	config.Addresses = []string{address}
	config.Username = "open_splunk"
	config.Password = password
	store, err := Open(config, fixedRetention(30*24*time.Hour))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	indexTime := time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.UTC)
	event := testStoredEvent("native-event", "main", indexTime)
	event.BatchID = "native-batch"
	event.Event.Raw = []byte{0xff, 0, 'b', 'y', 't', 'e', 's'}
	event.Event.RawEncoding = 2
	event.Event.Service = stringPointer("")
	event.Event.Level = nil
	event.Event.Fields = typedObjectValue(
		typedField("signed", typedSint(-1<<63)),
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("nothing", typedNull()),
		typedField("literal.dot", typedString("literal")),
		typedField("nested", typedObject(typedField("value", typedString("nested")))),
		typedField("mixed", typedList(typedSint(1), typedString("two"), typedBool(true))),
		typedField("bytes", typedBytes([]byte{0, 0xff, 0x10})),
	)
	batch := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "native-batch", BatchSequence: 1,
		Events: []*ingest.StoredEvent{event},
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := store.Store(ctx, batch); err != nil {
			t.Fatalf("Store attempt %d: %v", attempt, err)
		}
	}

	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queryConnection.Close() })
	var count uint64
	if err := queryConnection.QueryRow(ctx, "SELECT count() FROM open_splunk.events WHERE event_id = ?", "native-event").Scan(&count); err != nil {
		t.Fatalf("query dedup count: %v", err)
	}
	if count != 1 {
		t.Fatalf("retry stored %d rows, want 1", count)
	}

	var (
		raw                                           []byte
		signedType, unsignedType, nullType, mixedType string
		bytesType, literalDot, nestedValue            string
		servicePresent, serviceEmpty, levelMissing    bool
		fieldNames                                    []string
		expiresAt                                     time.Time
	)
	query := "SELECT raw, dynamicType(fields.signed), dynamicType(fields.unsigned), " +
		"dynamicType(fields.nothing), dynamicType(fields.mixed), dynamicType(fields.bytes), " +
		"fields.`literal%2Edot`.:String, fields.nested.value.:String, " +
		"isNotNull(service), service = '', isNull(level), field_names, expires_at " +
		"FROM open_splunk.events WHERE event_id = ? LIMIT 1"
	if err := queryConnection.QueryRow(ctx, query, "native-event").Scan(
		&raw, &signedType, &unsignedType, &nullType, &mixedType, &bytesType,
		&literalDot, &nestedValue, &servicePresent, &serviceEmpty, &levelMissing,
		&fieldNames, &expiresAt,
	); err != nil {
		t.Fatalf("query stored event: %v", err)
	}
	if !bytes.Equal(raw, event.Event.Raw) {
		t.Fatalf("raw = %x, want %x", raw, event.Event.Raw)
	}
	if signedType != "Int64" || unsignedType != "UInt64" || nullType != "None" ||
		mixedType != "Array(Dynamic)" || bytesType != "Map(String, String)" {
		t.Fatalf("dynamic types = %q %q %q %q %q", signedType, unsignedType, nullType, mixedType, bytesType)
	}
	if literalDot != "literal" || nestedValue != "nested" || !servicePresent || !serviceEmpty || !levelMissing {
		t.Fatalf("stored values literal=%q nested=%q servicePresent=%v serviceEmpty=%v levelMissing=%v",
			literalDot, nestedValue, servicePresent, serviceEmpty, levelMissing)
	}
	wantNames := []string{"bytes", "literal\\.dot", "mixed", "nested.value", "nothing", "signed", "unsigned"}
	if strings.Join(fieldNames, "\n") != strings.Join(wantNames, "\n") {
		t.Fatalf("field_names = %#v, want %#v", fieldNames, wantNames)
	}
	if want := indexTime.Truncate(time.Millisecond).Add(30 * 24 * time.Hour); !expiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", expiresAt, want)
	}
}

func integrationRandomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}

func integrationDocker(t *testing.T, ctx context.Context, stdin *bytes.Reader, args ...string) {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		command.Stdin = stdin
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func integrationWaitForClickHouse(t *testing.T, ctx context.Context, container, password string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	stable := 0
	var last string
	for time.Now().Before(deadline) {
		command := exec.CommandContext(ctx, "docker", "exec", container, "clickhouse-client",
			"--user", "open_splunk", "--password", password, "--query", "SELECT 1")
		output, err := command.CombinedOutput()
		last = fmt.Sprintf("%v: %s", err, output)
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for ClickHouse: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("ClickHouse did not become ready: %s", last)
}

func integrationNativeAddress(t *testing.T, ctx context.Context, container string) string {
	t.Helper()
	output, err := exec.CommandContext(ctx, "docker", "port", container, "9000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve native port: %v: %s", err, output)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	t.Fatalf("Docker returned no loopback native address: %s", output)
	return ""
}
