package queryexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const queryExecutorIntegrationImage = "clickhouse/clickhouse-server:26.3.17.4"

// TestExecutorAndManagerAgainstClickHouse is opt-in because it starts an
// ephemeral Docker container and may pull the pinned ClickHouse image.
func TestExecutorAndManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container := "open-splunk-queryexec-" + queryIntegrationRandomHex(t, 6)
	password := queryIntegrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = queryExecutorIntegrationImage
	}
	queryIntegrationDocker(t, ctx, nil,
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
	queryIntegrationWait(t, ctx, container, password)
	queryIntegrationMigrate(t, ctx, container, password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{queryIntegrationNativeAddress(t, ctx, container)},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	executor, err := New(connection, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("native typed scan types", func(t *testing.T) {
		sink := &fakeSink{}
		err := executor.Execute(ctx, clickhouse.CompiledQuery{
			SQL: "SELECT toDecimal128('123.4500', 4) AS amount, " +
				"CAST('12:34:56.123456' AS Time64(6)) AS elapsed, " +
				"CAST(42 AS Dynamic) AS choice, CAST('{\"name\":\"alice\",\"count\":2}' AS JSON) AS payload",
			OutputFields: []string{"amount", "elapsed", "choice", "payload"},
		}, sink)
		if err != nil {
			t.Fatal(err)
		}
		wantKinds := []searchjobs.ValueKind{
			searchjobs.ValueKindDecimal, searchjobs.ValueKindDuration,
			searchjobs.ValueKindMixed, searchjobs.ValueKindObject,
		}
		for index, want := range wantKinds {
			if sink.schema.Columns[index].Kind != want {
				t.Fatalf("column %d kind = %v, want %v", index, sink.schema.Columns[index].Kind, want)
			}
		}
		if got, ok := sink.rows[0][0].Decimal(); !ok || got != "123.45" {
			t.Fatalf("decimal = %q, %v", got, ok)
		}
		if got, ok := sink.rows[0][1].Duration(); !ok || got != 12*time.Hour+34*time.Minute+56*time.Second+123456*time.Microsecond {
			t.Fatalf("duration = %v, %v", got, ok)
		}
		if got, ok := sink.rows[0][2].Unsigned(); !ok || got != 42 {
			t.Fatalf("dynamic = %d, %v", got, ok)
		}
		if fields, ok := sink.rows[0][3].Object(); !ok || len(fields) != 2 {
			t.Fatalf("JSON = %#v", sink.rows[0][3])
		}
	})

	eventIndexTime := queryIntegrationInsertEvent(t, ctx, connection)
	t.Run("manager pipeline and stable page", func(t *testing.T) {
		manager, err := searchjobs.New(searchjobs.Config{
			Executor:        executor,
			Snapshotter:     queryIntegrationSnapshotter(1),
			Compiler:        clickhouse.Compiler{},
			MaxConcurrent:   1,
			MaxQueued:       1,
			CleanupInterval: -1,
			NewID:           func() string { return "queryexec-integration-job" },
			CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		now := time.Now().UTC()
		job, err := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:               "index=main | table message",
			OwnerID:           "owner",
			TenantID:          "tenant",
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			Earliest:          now.Add(-time.Hour),
			Latest:            now.Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		completed := queryIntegrationWaitForTerminal(t, manager, job.ID)
		if completed.State != searchjobs.StateCompleted {
			t.Fatalf("job state = %v, failure=%#v", completed.State, completed.Failure)
		}
		page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(page.Rows))
		}
		if got, ok := page.Rows[0].Values[0].String(); !ok || got != "manager integration" {
			t.Fatalf("message = %q, %v", got, ok)
		}
	})

	t.Run("stats manager pipeline and typed schema", func(t *testing.T) {
		manager, err := searchjobs.New(searchjobs.Config{
			Executor:        executor,
			Snapshotter:     queryIntegrationSnapshotter(1),
			Compiler:        clickhouse.Compiler{},
			MaxConcurrent:   1,
			MaxQueued:       1,
			CleanupInterval: -1,
			// The event and cutoff deliberately share a wall-clock second. A bare
			// time.Time bind is inferred as second-precision DateTime and used to
			// exclude this already-committed DateTime64(3) event.
			Now:       func() time.Time { return eventIndexTime.Add(500 * time.Microsecond) },
			NewID:     func() string { return "queryexec-stats-integration-job" },
			CursorKey: []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		now := eventIndexTime.Add(500 * time.Microsecond)
		job, err := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:               "index=main | stats count AS events by host",
			OwnerID:           "owner",
			TenantID:          "tenant",
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			Earliest:          now.Add(-time.Hour),
			Latest:            now.Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		completed := queryIntegrationWaitForTerminal(t, manager, job.ID)
		if completed.State != searchjobs.StateCompleted {
			t.Fatalf("job state = %v, failure=%#v", completed.State, completed.Failure)
		}
		page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Schema.Columns) != 2 || page.Schema.Columns[0].Name != "host" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindString || page.Schema.Columns[1].Name != "events" ||
			page.Schema.Columns[1].Kind != searchjobs.ValueKindUnsigned {
			t.Fatalf("stats schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("stats rows = %d, want 1", len(page.Rows))
		}
		if host, ok := page.Rows[0].Values[0].String(); !ok || host != "host" {
			t.Fatalf("stats host = %q, %v", host, ok)
		}
		if count, ok := page.Rows[0].Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("stats count = %d, %v", count, ok)
		}
	})

	t.Run("cancellation reaches native query", func(t *testing.T) {
		cancelCtx, cancelQuery := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancelQuery()
		started := time.Now()
		err := executor.Execute(cancelCtx, clickhouse.CompiledQuery{
			SQL:          "SELECT sleep(2) AS waited",
			OutputFields: []string{"waited"},
		}, &fakeSink{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled query error = %v", err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("canceled query returned after %v", elapsed)
		}
	})
}

type queryIntegrationSnapshotter uint64

func (snapshotter queryIntegrationSnapshotter) VisibilityCutoff(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(snapshotter), nil
}

func queryIntegrationInsertEvent(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) time.Time {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second).Add(987 * time.Millisecond)
	message := "manager integration"
	document := clickhousedriver.NewJSON()
	if err := batch.Append(
		"queryexec-event", "tenant", "main", now, now,
		nil, uint8(1), "host", "source", "test", nil, uint8(1), nil, &message, []byte(message),
		uint8(1), nil, nil, document, []string{}, "collector", "batch", uint64(1),
		now.Add(24*time.Hour), uint64(1),
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return now
}

func queryIntegrationWaitForTerminal(t *testing.T, manager *searchjobs.Manager, id string) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("search job did not reach a terminal state")
	return searchjobs.Job{}
}

func queryIntegrationMigrate(t *testing.T, ctx context.Context, container, password string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", paths, err)
	}
	var migrations bytes.Buffer
	for _, path := range paths {
		migration, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	queryIntegrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
}

func queryIntegrationRandomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}

func queryIntegrationDocker(t *testing.T, ctx context.Context, stdin *bytes.Reader, args ...string) {
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

func queryIntegrationWait(t *testing.T, ctx context.Context, container, password string) {
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

func queryIntegrationNativeAddress(t *testing.T, ctx context.Context, container string) string {
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
