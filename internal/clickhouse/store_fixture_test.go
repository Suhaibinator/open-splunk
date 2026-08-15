package clickhouse

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

// startClickHouseStoreFixture starts the repository's shared pinned container,
// applies the real migrations, and returns a query connection and store writer.
func startClickHouseStoreFixture(
	t *testing.T,
	ctx context.Context,
) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container, err := testsupport.StartClickHouse(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("start pinned ClickHouse fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := container.Close(cleanupCtx); err != nil {
			t.Errorf("close ClickHouse fixture: %v", err)
		}
	})

	migrationPaths, err := filepath.Glob(
		filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"),
	)
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	command := exec.CommandContext(
		ctx,
		"docker", "exec", "--interactive", container.Name, "clickhouse-client",
		"--user", container.Username, "--password", container.Password, "--multiquery",
	)
	command.Stdin = bytes.NewReader(migrations.Bytes())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("apply ClickHouse fixture migrations: %v\n%s", err, output)
	}

	config := DefaultConfig()
	config.Addresses = []string{container.Address}
	config.Username = container.Username
	config.Password = container.Password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open fixture visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create fixture visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	store, err := Open(config, fixedRetention(100*365*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("open ClickHouse fixture store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping ClickHouse fixture store: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("resolve ClickHouse fixture options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse fixture query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}
